#include <Common/Exception.h>

#include <DataStreams/IBlockInputStream.h>

#include <Storages/StorageFactory.h>
#include <Storages/StorageWindow.h>

#include <Columns/ColumnTuple.h>
#include <Core/iostream_debug_helpers.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}


class WindowBlockInputStream : public IBlockInputStream
{
public:
    WindowBlockInputStream(const Names & column_names_, StorageWindow & storage_, BlockInputStreamPtr & in_, Block && sample_block_)
        : column_names(column_names_), storage(storage_), in(in_), sample_block(std::move(sample_block_))
    {
    }

    String getName() const override { return "Window"; }

    Block getHeader() const override { return sample_block; }

    ~WindowBlockInputStream() override { storage.data.clear(); }

protected:
    void readPrefix() override { in->readPrefix(); }

    Block readImpl() override
    {
        auto block = in->read();
        if (!block)
            return {};
        auto res = getHeader();
        storage.data.push_back(block);
        if (storage.data.size() < storage.window_size)
            return res;
        if (storage.data.size() > storage.window_size)
            storage.data.pop_front();

        for (auto & ctn : res)
        {
            Columns columns;
            for (auto & b : storage.data)
            {
                auto & sub_ctn = b.getByName(ctn.name);
                columns.push_back(sub_ctn.column);
            }
            ctn.column = ColumnTuple::create(columns);
        }
        return res;
    }

    void readSuffix() override { in->readSuffix(); }

private:
    Names column_names;
    StorageWindow & storage;
    BlockInputStreamPtr in;
    Block sample_block;
};


class WindowBlockOutputStream : public IBlockOutputStream
{
public:
    explicit WindowBlockOutputStream(StorageWindow & storage_, const BlockOutputStreamPtr & out_) : storage(storage_), out(out_) { }

    Block getHeader() const override { return storage.getSampleBlock(); }

    void writePrefix() override { out->writePrefix(); }

    void write(const Block & block) override
    {
        if (!block)
            return;
        if (block.rows())
        {
            storage.check(block, true);
            storage.data.pop_back();
            storage.data.push_back(block);
            out->write(block);
        }
        else
        {
            if (storage.data.size() < storage.window_size)
                out->write(storage.data.back());
        }
    }

    void writeSuffix() override { out->writeSuffix(); }

private:
    StorageWindow & storage;
    BlockOutputStreamPtr out;
};


StorageWindow::StorageWindow(
    String database_name_,
    String table_name_,
    ColumnsDescription columns_description_,
    String source_table_,
    String dest_table_,
    UInt64 window_size_,
    const Context & context_)
    : database_name(std::move(database_name_))
    , table_name(std::move(table_name_))
    , source_table(std::move(source_table_))
    , dest_table(std::move(dest_table_))
    , window_size(window_size_)
    , global_context(context_)
{
    setColumns(std::move(columns_description_));
}


BlockInputStreams StorageWindow::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned /* num_streams */)
{
    auto table = context.getTable("", source_table);
    return table->read(column_names, query_info, context, processed_stage, max_block_size, 1);
}


BlockOutputStreamPtr StorageWindow::write(const ASTPtr & query, const Context & context)
{
    if (dest_table == "")
        return std::make_shared<WindowBlockOutputStream>(*this, std::make_shared<NullBlockOutputStream>(Block{}));
    auto table = context.getTable("", dest_table);
    auto out = table->write(query, context);
    return std::make_shared<WindowBlockOutputStream>(*this, out);
}


void registerStorageWindow(StorageFactory & factory)
{
    factory.registerStorage("Window", [](const StorageFactory::Arguments & args) {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 3)
            throw Exception(
                "Storage Window requires exactly 3 parameters"
                " - regexp for source table names, dest table name and window size.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        engine_args[0] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[0], args.local_context);
        engine_args[1] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[1], args.local_context);
        engine_args[2] = evaluateConstantExpressionAsLiteral(engine_args[2], args.local_context);


        auto source_table = engine_args[0]->as<ASTLiteral &>().value.safeGet<String>();
        auto dest_table = engine_args[1]->as<ASTLiteral &>().value.safeGet<String>();
        auto window_size = engine_args[2]->as<ASTLiteral &>().value.safeGet<UInt64>();
        if (window_size == 0)
            throw Exception("Window size cannot be 0", ErrorCodes::LOGICAL_ERROR);

        return StorageWindow::create(
            args.database_name, args.table_name, args.columns, source_table, dest_table, window_size, args.context);
    });
}

StorageWindowWrapper::StorageWindowWrapper(const String & name_, StorageWindow & storage_) : name(name_), storage(storage_)
{
    auto columns_description_ = storage.getColumns();
    for (auto nt : columns_description_.getAll())
    {
        columns_description_.modify(nt.name, [&](ColumnDescription & column) {
            DataTypes types(storage.window_size, nt.type);
            column.type = std::make_shared<DataTypeTuple>(types);
        });
    }
    setColumns(std::move(columns_description_));
};

BlockInputStreams StorageWindowWrapper::read(
    const Names & column_names,
    const SelectQueryInfo & query_info,
    const Context & context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned /* num_streams */)
{
    auto table = context.getTable("", storage.source_table);
    auto in = table->read(column_names, query_info, context, processed_stage, max_block_size, 1);
    if (in.empty())
        return {};
    return {std::make_shared<WindowBlockInputStream>(column_names, storage, in[0], getSampleBlockForColumns(column_names))};
}

}
