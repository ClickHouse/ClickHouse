#include <fstream>
#include <sstream>
#include <boost/filesystem.hpp>

#include <Storages/StorageCatBoostPool.h>
#include <DataStreams/IProfilingBlockInputStream.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/FilterColumnsBlockInputStream.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTIdentifier.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_PARSE_TEXT;
    extern const int DATABASE_ACCESS_DENIED;
}

namespace
{
class CatBoostDatasetBlockInputStream : public IProfilingBlockInputStream
{
public:

    CatBoostDatasetBlockInputStream(const std::string & file_name, const std::string & format_name,
                                    const Block & sample_block, const Context & context, size_t max_block_size)
            : file_name(file_name), format_name(format_name)
    {
        read_buf = std::make_unique<ReadBufferFromFile>(file_name);
        reader = FormatFactory::instance().getInput(format_name, *read_buf, sample_block, context, max_block_size);
    }

    String getName() const override
    {
        return "CatBoostDataset";
    }

    Block readImpl() override
    {
        return reader->read();
    }

    void readPrefixImpl() override
    {
        reader->readPrefix();
    }

    void readSuffixImpl() override
    {
        reader->readSuffix();
    }

    Block getHeader() const override { return reader->getHeader(); }

private:
    std::unique_ptr<ReadBufferFromFileDescriptor> read_buf;
    BlockInputStreamPtr reader;
    std::string file_name;
    std::string format_name;
};

}

static boost::filesystem::path canonicalPath(std::string && path)
{
    return boost::filesystem::canonical(boost::filesystem::path(path));
}

static std::string resolvePath(const boost::filesystem::path & base_path, std::string && path)
{
    boost::filesystem::path resolved_path(path);
    if (!resolved_path.is_absolute())
        return boost::filesystem::canonical(resolved_path, base_path).string();
    return boost::filesystem::canonical(resolved_path).string();
}

static void checkCreationIsAllowed(const String & base_path, const String & path)
{
    if (base_path != path.substr(0, base_path.size()))
        throw Exception(
            "Using file descriptor or user specified path as source of storage isn't allowed for server daemons",
            ErrorCodes::DATABASE_ACCESS_DENIED);
}


StorageCatBoostPool::StorageCatBoostPool(const Context & context,
                                         String column_description_file_name_,
                                         String data_description_file_name_)
        : column_description_file_name(std::move(column_description_file_name_)),
          data_description_file_name(std::move(data_description_file_name_))
{
    auto base_path = canonicalPath(context.getPath());
    column_description_file_name = resolvePath(base_path, std::move(column_description_file_name));
    data_description_file_name = resolvePath(base_path, std::move(data_description_file_name));
    if (context.getApplicationType() == Context::ApplicationType::SERVER)
    {
        const auto & base_path_str = base_path.string();
        checkCreationIsAllowed(base_path_str, column_description_file_name);
        checkCreationIsAllowed(base_path_str, data_description_file_name);
    }

    parseColumnDescription();
    createSampleBlockAndColumns();
}

std::string StorageCatBoostPool::getColumnTypesString(const ColumnTypesMap & columnTypesMap)
{
    std::string types_string;
    bool first = true;
    for (const auto & value : columnTypesMap)
    {
        if (!first)
            types_string.append(", ");

        first = false;
        types_string += value.first;
    }

    return types_string;
}

void StorageCatBoostPool::checkDatasetDescription()
{
    std::ifstream in(data_description_file_name);
    if (!in.good())
        throw Exception("Cannot open file: " + data_description_file_name, ErrorCodes::CANNOT_OPEN_FILE);

    std::string line;
    if (!std::getline(in, line))
        throw Exception("File is empty: " + data_description_file_name, ErrorCodes::CANNOT_PARSE_TEXT);

    size_t columns_count = 1;
    for (char sym : line)
        if (sym == '\t')
            ++columns_count;

    columns_description.resize(columns_count);
}

void StorageCatBoostPool::parseColumnDescription()
{
    /// NOTE: simple parsing
    /// TODO: use ReadBufferFromFile

    checkDatasetDescription();

    std::ifstream in(column_description_file_name);
    if (!in.good())
        throw Exception("Cannot open file: " + column_description_file_name, ErrorCodes::CANNOT_OPEN_FILE);

    std::string line;
    size_t line_num = 0;
    auto column_types_map = getColumnTypesMap();
    auto column_types_string = getColumnTypesString(column_types_map);

    /// Enumerate default names for columns as Auxiliary, Auxiliary1, Auxiliary2, ...
    std::map<DatasetColumnType, size_t> columns_per_type_count;

    while (std::getline(in, line))
    {
        ++line_num;
        std::string str_line_num = std::to_string(line_num);

        if (line.empty())
            continue;

        std::istringstream iss(line);
        std::vector<std::string> tokens;
        std::string token;
        while (std::getline(iss, token, '\t'))
            tokens.push_back(token);

        if (tokens.size() != 2 && tokens.size() != 3)
            throw Exception("Cannot parse column description at line " + str_line_num + " '" + line + "' "
                            + ": expected 2 or 3 columns, got " + std::to_string(tokens.size()),
                            ErrorCodes::CANNOT_PARSE_TEXT);

        std::string str_id = tokens[0];
        std::string col_type = tokens[1];
        std::string col_alias = tokens.size() > 2 ? tokens[2] : "";

        size_t num_id;
        try
        {
            num_id = std::stoull(str_id);
        }
        catch (std::exception & e)
        {
            throw Exception("Cannot parse column index at row " + str_line_num + ": " + e.what(),
                            ErrorCodes::CANNOT_PARSE_TEXT);
        }

        if (num_id >= columns_description.size())
            throw Exception("Invalid index at row  " + str_line_num + ": " + str_id
                            + ", expected in range [0, " + std::to_string(columns_description.size()) + ")",
                            ErrorCodes::CANNOT_PARSE_TEXT);

        if (column_types_map.count(col_type) == 0)
            throw Exception("Invalid column type: " + col_type + ", expected: " + column_types_string,
                            ErrorCodes::CANNOT_PARSE_TEXT);

        auto type = column_types_map[col_type];

        std::string col_name;

        bool is_feature_column = type == DatasetColumnType::Num || type == DatasetColumnType::Categ;
        auto & col_number = columns_per_type_count[type];
        /// If column is not feature skip '0' after the name (to use 'Target' instead of 'Target0').
        col_name = col_type + (is_feature_column || col_number ? std::to_string(col_number) : "");
        ++col_number;

        columns_description[num_id] = ColumnDescription(col_name, col_alias, type);
    }
}

void StorageCatBoostPool::createSampleBlockAndColumns()
{
    ColumnsDescription columns;
    NamesAndTypesList cat_columns;
    NamesAndTypesList num_columns;
    sample_block.clear();
    for (auto & desc : columns_description)
    {
        DataTypePtr type;
        if (desc.column_type == DatasetColumnType::Categ
            || desc.column_type == DatasetColumnType::Auxiliary
            || desc.column_type == DatasetColumnType::DocId)
            type = std::make_shared<DataTypeString>();
        else
            type = std::make_shared<DataTypeFloat64>();

        if (desc.column_type == DatasetColumnType::Categ)
            cat_columns.emplace_back(desc.column_name, type);
        else if (desc.column_type == DatasetColumnType::Num)
            num_columns.emplace_back(desc.column_name, type);
        else
            columns.materialized.emplace_back(desc.column_name, type);

        if (!desc.alias.empty())
        {
            auto alias = std::make_shared<ASTIdentifier>(desc.column_name);
            columns.defaults[desc.alias] = {ColumnDefaultKind::Alias, alias};
            columns.aliases.emplace_back(desc.alias, type);
        }

        sample_block.insert(ColumnWithTypeAndName(type, desc.column_name));
    }
    columns.ordinary.insert(columns.ordinary.end(), num_columns.begin(), num_columns.end());
    columns.ordinary.insert(columns.ordinary.end(), cat_columns.begin(), cat_columns.end());

    setColumns(columns);
}

BlockInputStreams StorageCatBoostPool::read(const Names & column_names,
                       const SelectQueryInfo & /*query_info*/,
                       const Context & context,
                       QueryProcessingStage::Enum /*processed_stage*/,
                       size_t max_block_size,
                       unsigned /*threads*/)
{
    auto stream = std::make_shared<CatBoostDatasetBlockInputStream>(
            data_description_file_name, "TSV", sample_block, context, max_block_size);

    auto filter_stream = std::make_shared<FilterColumnsBlockInputStream>(stream, column_names, false);
    return { filter_stream };
}

}
