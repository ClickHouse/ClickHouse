#include <Dictionaries/DictionarySource.h>
#include <Dictionaries/DictionaryHelpers.h>
#include <Processors/ISource.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

class DictionarySource : public ISource
{
public:

    explicit DictionarySource(std::shared_ptr<DictionarySourceCoordinator> coordinator_)
        : ISource(coordinator_->getHeader()), coordinator(std::move(coordinator_))
    {
    }

private:
    String getName() const override { return "DictionarySource"; }

    Chunk generate() override
    {
        ColumnsWithTypeAndName key_columns_to_read;
        ColumnsWithTypeAndName data_columns;

        if (!coordinator->getKeyColumnsNextRangeToRead(key_columns_to_read, data_columns))
            return {};

        const auto & header = coordinator->getHeader();

        std::vector<ColumnPtr> key_columns;
        std::vector<DataTypePtr> key_types;

        key_columns.reserve(key_columns_to_read.size());
        key_types.reserve(key_columns_to_read.size());

        std::unordered_map<std::string_view, ColumnPtr> name_to_column;

        for (const auto & key_column_to_read : key_columns_to_read)
        {
            key_columns.emplace_back(key_column_to_read.column);
            key_types.emplace_back(key_column_to_read.type);

            if (header.has(key_column_to_read.name))
                name_to_column.emplace(key_column_to_read.name, key_column_to_read.column);
        }

        for (const auto & data_column : data_columns)
        {
            if (header.has(data_column.name))
                name_to_column.emplace(data_column.name, data_column.column);
        }

        const auto & attributes_names_to_read = coordinator->getAttributesNamesToRead();
        const auto & attributes_types_to_read = coordinator->getAttributesTypesToRead();
        const auto & attributes_default_values_columns = coordinator->getAttributesDefaultValuesColumns();

        const auto & read_columns_func = coordinator->getReadColumnsFunc();
        auto attributes_columns = read_columns_func(
            attributes_names_to_read,
            attributes_types_to_read,
            key_columns,
            key_types,
            attributes_default_values_columns);

        for (size_t i = 0; i < attributes_names_to_read.size(); ++i)
        {
            const auto & attribute_name = attributes_names_to_read[i];
            name_to_column.emplace(attribute_name, attributes_columns[i]);
        }

        std::vector<ColumnPtr> result_columns;
        result_columns.reserve(header.columns());

        for (const auto & column_with_type : header)
        {
            const auto & header_name = column_with_type.name;
            auto it = name_to_column.find(header_name);
            if (it == name_to_column.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column name {} not found in result columns", header_name);

            result_columns.emplace_back(it->second);
        }

        size_t rows_size = result_columns[0]->size();
        return Chunk(result_columns, rows_size);
    }

    std::shared_ptr<DictionarySourceCoordinator> coordinator;
};

bool DictionarySourceCoordinator::getKeyColumnsNextRangeToRead(ColumnsWithTypeAndName & key_columns, ColumnsWithTypeAndName & data_columns)
{
    size_t read_block_index = parallel_read_block_index++;

    size_t start = max_block_size * read_block_index;
    size_t end = max_block_size * (read_block_index + 1);

    size_t keys_size = key_columns_with_type[0].column->size();

    if (start >= keys_size)
        return false;

    end = std::min(end, keys_size);
    size_t length = end - start;

    key_columns = cutColumns(key_columns_with_type, start, length);
    data_columns = cutColumns(data_columns_with_type, start, length);

    return true;
}

void DictionarySourceCoordinator::initialize(const Names & column_names)
{
    ColumnsWithTypeAndName columns_with_type;

    const auto & dictionary_structure = dictionary->getStructure();

    for (const auto & column_name : column_names)
    {
        ColumnWithTypeAndName column_with_type;
        column_with_type.name = column_name;

        auto it = dictionary_structure.attribute_name_to_index.find(column_name);
        if (it == dictionary_structure.attribute_name_to_index.end())
        {
            if (dictionary_structure.id.has_value() && column_name == dictionary_structure.id->name)
            {
                column_with_type.type = std::make_shared<DataTypeUInt64>();
            }
            else if (dictionary_structure.range_min.has_value() && column_name == dictionary_structure.range_min->name)
            {
                column_with_type.type = dictionary_structure.range_min->type;
            }
            else if (dictionary_structure.range_max.has_value() && column_name == dictionary_structure.range_max->name)
            {
                column_with_type.type = dictionary_structure.range_max->type;
            }
            else if (dictionary_structure.key.has_value())
            {
                const auto & dictionary_key_attributes = *dictionary_structure.key;
                for (const auto & attribute : dictionary_key_attributes)
                {
                    if (column_name == attribute.name)
                    {
                        column_with_type.type = attribute.type;
                        break;
                    }
                }
            }
        }
        else
        {
            const auto & attribute = dictionary_structure.attributes[it->second];
            attributes_names_to_read.emplace_back(attribute.name);
            attributes_types_to_read.emplace_back(attribute.type);
            attributes_default_values_columns.emplace_back(nullptr);

            column_with_type.type = attribute.type;
        }

        if (!column_with_type.type)
            throw Exception(ErrorCodes::NO_SUCH_COLUMN_IN_TABLE, "No such column name {} in dictionary {}",
                column_name,
                dictionary->getDictionaryID().getNameForLogs());

        column_with_type.column = column_with_type.type->createColumn();
        columns_with_type.emplace_back(std::move(column_with_type));
    }

    header = Block(std::move(columns_with_type));
}

ColumnsWithTypeAndName
DictionarySourceCoordinator::cutColumns(const ColumnsWithTypeAndName & columns_with_type, size_t start, size_t length)
{
    ColumnsWithTypeAndName result;
    result.reserve(columns_with_type.size());

    for (const auto & column_with_type : columns_with_type)
    {
        ColumnWithTypeAndName result_column_with_type;

        result_column_with_type.column = column_with_type.column->cut(start, length);
        result_column_with_type.type = column_with_type.type;
        result_column_with_type.name = column_with_type.name;

        result.emplace_back(std::move(result_column_with_type));
    }

    return result;
}

Pipe DictionarySourceCoordinator::read(size_t num_streams)
{
    Pipes pipes;
    pipes.reserve(num_streams);

    auto coordinator = shared_from_this();

    for (size_t i = 0; i < num_streams; ++i)
    {
        auto source = std::make_shared<DictionarySource>(coordinator);
        pipes.emplace_back(Pipe(std::move(source)));
    }

    return Pipe::unitePipes(std::move(pipes));
}

}
