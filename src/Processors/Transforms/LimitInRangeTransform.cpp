#include <algorithm>
#include <Processors/Transforms/LimitInRangeTransform.h>

#include <Interpreters/ExpressionActions.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnSparse.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

Block LimitInRangeTransform::transformHeader(
            Block header,
            const ActionsDAG * from_expression,
            const ActionsDAG * to_expression,
            const String & from_filter_column_name,
            const String & to_filter_column_name,
            bool remove_filter_column)
{
    std::cerr << "from_filter_column_name=" << from_filter_column_name << '\n';
    std::cerr << "to_filter_column_name=" << to_filter_column_name << '\n';

    std::cerr << "Original header: " << header.dumpStructure() << " " << remove_filter_column << '\n';

    // remove_filter_column = true; // from ExpressionAnalyzer.cpp

    Block from_header = header.cloneEmpty(); // without clone is ok, the updateHeader knows some state(is deleting one column if from and to are used)
    Block to_header = header.cloneEmpty();

    ColumnWithTypeAndName new_from_column{nullptr, DataTypePtr(), ""};
    ColumnWithTypeAndName new_to_column{nullptr, DataTypePtr(), ""};

    if (from_expression) {
        from_header = from_expression->updateHeader(std::move(from_header));
        std::cerr << "Header after from expression: " << from_header.dumpStructure() << '\n';

        for (const auto& col : from_header) {
            if (!header.has(col.name)) {
                new_from_column = col;
                break;
            }
        }
    }

    if (to_expression) {
        to_header = to_expression->updateHeader(std::move(to_header));
        std::cerr << "Header after to expression: " << to_header.dumpStructure() << '\n';

        // Identify new column from the to_expression
        for (const auto& col : to_header) {
            if (!header.has(col.name)) {
                new_to_column = col;
                break;
            }
        }
    }

    if (new_from_column.name != "") {
        header.insert(new_from_column);
    }

    if (new_to_column.name != "") {
        header.insert(new_to_column);
    }

    std::cerr << "Header after adding missing ones: " << header.dumpStructure() << '\n';

    if (!from_filter_column_name.empty()) {
        auto from_filter_type = header.getByName(from_filter_column_name).type;
        if (!from_filter_type->onlyNull() && !isUInt8(removeNullable(removeLowCardinality(from_filter_type))))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "Illegal type {} of column {} for from_filter. Must be UInt8 or Nullable(UInt8).",
                from_filter_type->getName(), from_filter_column_name);

        if (remove_filter_column)
            header.erase(from_filter_column_name);
    //  else
    //      replaceFilterToConstant(header, filter_column_name);
    }


    if (!to_filter_column_name.empty()) { 
        auto to_filter_type = header.getByName(to_filter_column_name).type;
        if (!to_filter_type->onlyNull() && !isUInt8(removeNullable(removeLowCardinality(to_filter_type))))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "Illegal type {} of column {} for to_filter. Must be UInt8 or Nullable(UInt8).",
                to_filter_type->getName(), to_filter_column_name);

        if (remove_filter_column) 
            header.erase(to_filter_column_name);
    //  else
    //      replaceFilterToConstant(header, filter_column_name);        
    }    
    std::cerr << "TransformHeader ending structure: " << header.dumpStructure() << '\n';
    return header;
}


LimitInRangeTransform::LimitInRangeTransform(
    const Block & header_,
    ExpressionActionsPtr from_expression_,
    ExpressionActionsPtr to_expression_,
    String from_filter_column_name_,
    String to_filter_column_name_,
    bool remove_filter_column_,
    bool on_totals_,
    std::shared_ptr<std::atomic<size_t>> rows_filtered_)
    : ISimpleTransform(
            header_,
            transformHeader(header_, from_expression_ ? &from_expression_->getActionsDAG() : nullptr, to_expression_ ? &to_expression_->getActionsDAG() : nullptr, from_filter_column_name_, to_filter_column_name_, remove_filter_column_),
            true)
    , from_expression(std::move(from_expression_))
    , to_expression(std::move(to_expression_))
    , from_filter_column_name(std::move(from_filter_column_name_))
    , to_filter_column_name(std::move(to_filter_column_name_))
    , remove_filter_column(remove_filter_column_)
    , on_totals(on_totals_),
    rows_filtered(rows_filtered_)
{
    std::cerr << "LimitInRangeTransform initalized\n";
    std::cerr << "FROM_name: " << from_filter_column_name << ", " << "TO NAME: " << to_filter_column_name << '\n'; 
    transformed_header = getInputPort().getHeader();
    std::cerr << "Transformed header: " << transformed_header.dumpStructure() << '\n';
    
    Block from_header = transformed_header; // without clone is ok, the updateHeader knows some state(is deleting one column if from and to are used)
    Block to_header = transformed_header;

    ColumnWithTypeAndName new_from_column{nullptr, DataTypePtr(), ""};
    ColumnWithTypeAndName new_to_column{nullptr, DataTypePtr(), ""};

    if (from_expression) {
        from_expression->execute(from_header);
        std::cerr << "Header after from expression: " << from_header.dumpStructure() << '\n';

        for (const auto& col : from_header) {
            if (!transformed_header.has(col.name)) {
                new_from_column = col;
                break;
            }
        }
    }

    if (to_expression) {
        to_expression->execute(to_header);
        std::cerr << "Header after to expression: " << to_header.dumpStructure() << '\n';

        // Identify new column from the to_expression
        for (const auto& col : to_header) {
            if (!transformed_header.has(col.name)) {
                new_to_column = col;
                break;
            }
        }
    }

    if (new_from_column.name != "") {
        transformed_header.insert(new_from_column);
        from_filter_column_position = transformed_header.getPositionByName(from_filter_column_name);
        auto & from_column = transformed_header.getByPosition(from_filter_column_position).column;
        if (from_column)
            constant_filter_description = ConstantFilterDescription(*from_column); // need different constants
    }

    if (new_to_column.name != "") {
        transformed_header.insert(new_to_column);
        to_filter_column_position = transformed_header.getPositionByName(to_filter_column_name);
        auto & to_column = transformed_header.getByPosition(to_filter_column_position).column;
        if (to_column)
            constant_filter_description = ConstantFilterDescription(*to_column);
    }

    std::cerr << "after adding missing: " << transformed_header.dumpStructure() << '\n';
    

    std::cerr << "Constructor end\n";
}

IProcessor::Status LimitInRangeTransform::prepare()
{
    std::cerr << "IProcessor::Status FilterTransform::prepare\n";

    // TODO: need some optimization here

    // if (!on_totals
    //     && (constant_filter_description.always_false
    //         /// Optimization for `WHERE column in (empty set)`.
    //         /// The result will not change after set was created, so we can skip this check.
    //         /// It is implemented in prepare() stop pipeline before reading from input port.
    //         || (!are_prepared_sets_initialized && expression && expression->checkColumnIsAlwaysFalse(filter_column_name))))
    // {
    //     input.close();
    //     output.finish();
    //     return Status::Finished;
    // }

    auto status = ISimpleTransform::prepare();
    /// Until prepared sets are initialized, output port will be unneeded, and prepare will return PortFull.
    if (status != IProcessor::Status::PortFull)
        are_prepared_sets_initialized = true;

    return status;
}

void LimitInRangeTransform::removeFilterIfNeed(Chunk & chunk) const
{
    std::cerr << "FilterTransform::removeFilterIfNeed\n";
    if (chunk && remove_filter_column) {
        if (!from_filter_column_name.empty())
            chunk.erase(from_filter_column_position);
        if (!to_filter_column_name.empty())
            chunk.erase(to_filter_column_position);
    }
}

void LimitInRangeTransform::transform(Chunk & chunk)
{
    std::cerr << "In LimitInRangeTransform::transform\n";
    auto chunk_rows_before = chunk.getNumRows();
    std::cerr << chunk_rows_before;
    // doTransform(chunk);
    if (from_expression && to_expression) {
        // doFromAndToTransform(chunk);
    } else if (from_expression) {
        doFromTransform(chunk);
    } else {
        // doToTransform(chunk);
    }
    if (rows_filtered)
        *rows_filtered += chunk_rows_before - chunk.getNumRows();
}

void LimitInRangeTransform::doFromTransform(Chunk & chunk)
{
    std::cerr << "FilterTransform::doFromTransform\n";
    size_t num_rows_before_filtration = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    DataTypes types;
    // auto select_final_indices_info = getSelectByFinalIndices(chunk);

    {
        std::cerr << "dumped structure of block1: " << getInputPort().getHeader().dumpStructure() << '\n';
        Block block = getInputPort().getHeader().cloneWithColumns(columns);
        std::cerr << "dumped structure of block2: " << block.dumpStructure() << '\n';
        columns.clear();

        if (from_expression)
            from_expression->execute(block, num_rows_before_filtration);
        
        // for (auto& column : block.getColumns()) {
        //     for (size_t i = 0; i < column->size(); ++i) {
        //         std::cerr << column->getFloat64(i) << " ";
        //     }
        //     std::cerr << '\n';
        // }
        columns = block.getColumns();
        types = block.getDataTypes();
        std::cerr << "dumped structure of block end:" << block.dumpStructure() << '\n';



    }
    std::cerr << constant_filter_description.always_true << " " << constant_filter_description.always_false << '\n';
    
    // can be wraped into filterDescription
    ColumnPtr filter_column = columns[from_filter_column_position];
    ColumnPtr data_holder = nullptr; // Explicit initialization

    if (filter_column->isSparse()) {
        data_holder = recursiveRemoveSparse(filter_column->getPtr());
    }

    if (filter_column->lowCardinality() && !data_holder) { // Prevent overwriting if already set
        data_holder = filter_column->convertToFullColumnIfLowCardinality();
    }

    const auto & column = data_holder ? *data_holder : *filter_column;

    const IColumn::Filter * filter_concrete_column = nullptr;
    if (const ColumnUInt8 * concrete_column = typeid_cast<const ColumnUInt8 *>(&column))
    {
        filter_concrete_column = &concrete_column->getData();
    }

    // if (from_index_found) {
    //     chunk.setColumns(std::move(columns), columns[0]->size());
    //     removeFilterIfNeed(chunk);
    // }

    size_t index;
    auto it = std::find(filter_concrete_column->begin(), filter_concrete_column->end(), 1);
    if (it != filter_concrete_column->end()) {
        index = std::distance(filter_concrete_column->begin(), it);
        from_index_found = true;
        std::cerr << "FOUND INDEX: " << index << "\n";
    } else {
        // also can be checked with memory size = 0;
        std::cerr << "NOT FOUND\n";
        return;
    }

    size_t len = columns[0]->size() - index;
    for (auto& col : columns) {
        col = col->cut(index, len);
        // for (size_t i = 0; i < col->size(); ++i) {
        //     std::cerr << col->getFloat64(i) << ' ';
        // }
        // std::cerr << '\n';
    }

    std::cerr << "before chunk: " << chunk.dumpStructure() << '\n';
    chunk.setColumns(std::move(columns), len);
    std::cerr << "after chunk: " << chunk.dumpStructure() << '\n';

    removeFilterIfNeed(chunk);
    std::cerr << "after removing filter: " << chunk.dumpStructure() << '\n';

}



}
