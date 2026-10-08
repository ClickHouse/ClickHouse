#include <Processors/Formats/AggregateFunctionStatesFromValuesInputFormat.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/Arena.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

using Mode = AggregateFunctionStatesFromValuesInputFormat::Mode;

/// The type of the values the aggregate function takes: the argument type, or a Tuple of them if there are several
/// (an empty Tuple for functions without arguments, such as `count`), or an Array of the above in the `array` mode.
DataTypePtr getValuesType(const DataTypeAggregateFunction & type, Mode mode)
{
    const auto & argument_types = type.getArgumentsDataTypes();
    DataTypePtr value_type = argument_types.size() == 1 ? argument_types[0] : std::make_shared<DataTypeTuple>(argument_types);
    if (mode == Mode::Array)
        return std::make_shared<DataTypeArray>(value_type);
    return value_type;
}

/// Returns the same object if there is nothing to replace, so the callers can check for changes by comparing pointers.
DataTypePtr getTypeToParse(const DataTypePtr & type, Mode mode)
{
    if (const auto * type_aggregate_function = typeid_cast<const DataTypeAggregateFunction *>(type.get()))
        return getValuesType(*type_aggregate_function, mode);

    if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        auto nested = getTypeToParse(type_array->getNestedType(), mode);
        if (nested.get() == type_array->getNestedType().get())
            return type;
        return std::make_shared<DataTypeArray>(nested);
    }

    if (const auto * type_map = typeid_cast<const DataTypeMap *>(type.get()))
    {
        auto nested = getTypeToParse(type_map->getNestedType(), mode);
        if (nested.get() == type_map->getNestedType().get())
            return type;
        return std::make_shared<DataTypeMap>(nested);
    }

    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        DataTypes elements = type_tuple->getElements();
        bool changed = false;
        for (auto & element : elements)
        {
            auto new_element = getTypeToParse(element, mode);
            changed |= new_element.get() != element.get();
            element = std::move(new_element);
        }
        if (!changed)
            return type;
        if (type_tuple->hasExplicitNames())
            return std::make_shared<DataTypeTuple>(elements, type_tuple->getElementNames());
        return std::make_shared<DataTypeTuple>(elements);
    }

    return type;
}

ColumnPtr buildStates(const ColumnPtr & column, const DataTypeAggregateFunction & type, Mode mode)
{
    const AggregateFunctionPtr & function = type.getFunction();
    size_t num_rows = column->size();

    ColumnPtr values = column;
    const ColumnArray::Offsets * offsets = nullptr;
    if (mode == Mode::Array)
    {
        const auto & column_array = assert_cast<const ColumnArray &>(*column);
        values = column_array.getDataPtr();
        offsets = &column_array.getOffsets();
    }

    Columns argument_columns;
    if (type.getArgumentsDataTypes().size() == 1)
    {
        argument_columns.push_back(values);
    }
    else
    {
        const auto & column_tuple = assert_cast<const ColumnTuple &>(*values);
        for (size_t i = 0; i < column_tuple.tupleSize(); ++i)
            argument_columns.push_back(column_tuple.getColumnPtr(i));
    }

    /// The function is created for the argument types without LowCardinality, as usual for aggregation.
    std::vector<const IColumn *> argument_column_ptrs;
    argument_column_ptrs.reserve(argument_columns.size());
    for (auto & argument_column : argument_columns)
    {
        argument_column = recursiveRemoveLowCardinality(argument_column->convertToFullColumnIfSparse());
        argument_column_ptrs.push_back(argument_column.get());
    }

    auto res = type.createColumn();
    auto & res_concrete = assert_cast<ColumnAggregateFunction &>(*res);
    auto & states = res_concrete.getData();
    Arena & arena = res_concrete.createOrGetArena();

    states.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
    {
        AggregateDataPtr place = arena.alignedAlloc(function->sizeOfData(), function->alignOfData());
        function->create(place);
        /// From now on the column owns the state and destroys it, also if something below throws.
        states.push_back(place);
    }

    if (mode == Mode::Array)
        function->addBatchArray(0, num_rows, states.data(), 0, argument_column_ptrs.data(), offsets->data(), &arena);
    else
        function->addBatch(0, num_rows, states.data(), 0, argument_column_ptrs.data(), &arena);

    return res;
}

/// `column` is what the underlying format has parsed for `parsed_type`; returns a column of `type`.
ColumnPtr buildStatesRecursively(const ColumnPtr & column, const DataTypePtr & parsed_type, const DataTypePtr & type, Mode mode)
{
    if (parsed_type.get() == type.get())
        return column;

    if (const auto * column_const = checkAndGetColumn<ColumnConst>(column.get()))
        return ColumnConst::create(buildStatesRecursively(column_const->getDataColumnPtr(), parsed_type, type, mode), column_const->size());

    if (const auto * type_aggregate_function = typeid_cast<const DataTypeAggregateFunction *>(type.get()))
        return buildStates(column, *type_aggregate_function, mode);

    if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        const auto & column_array = assert_cast<const ColumnArray &>(*column);
        const auto & parsed_type_array = assert_cast<const DataTypeArray &>(*parsed_type);
        return ColumnArray::create(
            buildStatesRecursively(column_array.getDataPtr(), parsed_type_array.getNestedType(), type_array->getNestedType(), mode),
            column_array.getOffsetsPtr());
    }

    if (const auto * type_map = typeid_cast<const DataTypeMap *>(type.get()))
    {
        const auto & column_map = assert_cast<const ColumnMap &>(*column);
        const auto & parsed_type_map = assert_cast<const DataTypeMap &>(*parsed_type);
        return ColumnMap::create(
            buildStatesRecursively(column_map.getNestedColumnPtr(), parsed_type_map.getNestedType(), type_map->getNestedType(), mode));
    }

    if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        const auto & column_tuple = assert_cast<const ColumnTuple &>(*column);
        const auto & parsed_type_tuple = assert_cast<const DataTypeTuple &>(*parsed_type);
        const auto & elements = type_tuple->getElements();
        const auto & parsed_elements = parsed_type_tuple.getElements();
        Columns columns(elements.size());
        for (size_t i = 0; i < elements.size(); ++i)
            columns[i] = buildStatesRecursively(column_tuple.getColumnPtr(i), parsed_elements[i], elements[i], mode);
        return ColumnTuple::create(std::move(columns));
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot build aggregate function states of type {} from the parsed values of type {}",
        type->getName(), parsed_type->getName());
}

}


std::optional<Block> AggregateFunctionStatesFromValuesInputFormat::getHeaderToParse(const Block & header, Mode mode)
{
    if (mode == Mode::State)
        return std::nullopt;

    std::optional<Block> res;
    for (size_t i = 0; i < header.columns(); ++i)
    {
        const auto & column = header.getByPosition(i);
        auto type_to_parse = getTypeToParse(column.type, mode);
        if (type_to_parse.get() == column.type.get())
            continue;

        if (!res)
            res = header;
        auto & column_to_parse = res->getByPosition(i);
        column_to_parse.type = type_to_parse;
        column_to_parse.column = column.column ? type_to_parse->createColumn() : nullptr;
    }
    return res;
}

AggregateFunctionStatesFromValuesInputFormat::AggregateFunctionStatesFromValuesInputFormat(
    SharedHeader header_, ReadBuffer * in_, InputFormatPtr underlying_, Mode mode_)
    : IInputFormat(std::move(header_), in_)
    , underlying(std::move(underlying_))
    , port(underlying->getPort().getHeader(), this)
    , mode(mode_)
{
    connect(underlying->getPort(), port);
    port.setNeeded();
}

Chunk AggregateFunctionStatesFromValuesInputFormat::read()
{
    Chunk chunk;
    bool has_chunk = false;
    while (!has_chunk)
    {
        IProcessor::Status status = underlying->prepare();
        switch (status)
        {
            case IProcessor::Status::Ready:
                underlying->work();
                break;
            case IProcessor::Status::Finished:
                return {};
            case IProcessor::Status::PortFull:
                chunk = port.pull();
                has_chunk = true;
                break;
            case IProcessor::Status::NeedData:
            case IProcessor::Status::Async:
            case IProcessor::Status::UpdatePipeline:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Input format {} returned status {}",
                    underlying->getName(), IProcessor::statusToName(status));
        }
    }

    if (!chunk)
        return chunk;

    const Block & header = getPort().getHeader();
    const Block & parsed_header = underlying->getPort().getHeader();
    size_t num_rows = chunk.getNumRows();
    Columns columns = chunk.detachColumns();
    for (size_t i = 0; i < columns.size(); ++i)
        columns[i] = buildStatesRecursively(columns[i], parsed_header.getByPosition(i).type, header.getByPosition(i).type, mode);
    chunk.setColumns(std::move(columns), num_rows);
    return chunk;
}

void AggregateFunctionStatesFromValuesInputFormat::resetParser()
{
    underlying->resetParser();
    IInputFormat::resetParser();
}

void AggregateFunctionStatesFromValuesInputFormat::setReadBuffer(ReadBuffer & in_)
{
    underlying->setReadBuffer(in_);
    IInputFormat::setReadBuffer(in_);
}

void AggregateFunctionStatesFromValuesInputFormat::resetReadBuffer()
{
    underlying->resetReadBuffer();
    IInputFormat::resetReadBuffer();
}

}
