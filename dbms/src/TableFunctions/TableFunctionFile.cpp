#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFile.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Common/escapeForFileName.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageFile.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <IO/copyData.h>
#include <IO/ReadBufferFromFile.h>
#include <Poco/Path.h>
#include <boost/algorithm/string.hpp>

#include <fcntl.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
        extern const int DATABASE_ACCESS_DENIED;
    }

    StoragePtr TableFunctionFile::executeImpl(const ASTPtr & ast_function, const Context & context) const
    {
        // Parse args
        ASTs & args_func = typeid_cast<ASTFunction &>(*ast_function).children;

        if (args_func.size() != 1)
            throw Exception("Table function 'file' must have arguments.", ErrorCodes::LOGICAL_ERROR);

        ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

        if (args.size() != 3 && args.size() != 4)
            throw Exception("Table function 'file' requires exactly 3 or 4 arguments: path, format, structure and useStorageMemory.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < 3; ++i)
            args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

        std::string path = static_cast<const ASTLiteral &>(*args[0]).value.safeGet<String>();
        std::string format = static_cast<const ASTLiteral &>(*args[1]).value.safeGet<String>();
        std::string structure = static_cast<const ASTLiteral &>(*args[2]).value.safeGet<String>();
        bool useStorageMemory = false;

        if (args.size() == 4)
            useStorageMemory = static_cast<const ASTLiteral &>(*args[2]).value.safeGet<bool>();

        std::string db_data_path = context.getPath() + "data/" + escapeForFileName(context.getCurrentDatabase());

        Poco::Path poco_path = Poco::Path(path);
        if (poco_path.isRelative())
            poco_path = Poco::Path(db_data_path, poco_path);

        std::string absolute_path = poco_path.absolute().toString();

        // Create sample block
        std::vector<std::string> structure_vals;
        boost::split(structure_vals, structure, boost::algorithm::is_any_of(" ,"), boost::algorithm::token_compress_on);

        if (structure_vals.size() & 1)
            throw Exception("Odd number of attributes in section structure", ErrorCodes::LOGICAL_ERROR);

        Block sample_block = Block();
        const DataTypeFactory & data_type_factory = DataTypeFactory::instance();

        for (size_t i = 0; i < structure_vals.size(); i += 2)
        {
            ColumnWithTypeAndName column;
            column.name = structure_vals[i];
            column.type = data_type_factory.get(structure_vals[i + 1]);
            column.column = column.type->createColumn();
            sample_block.insert(std::move(column));
        }

        // Create table
        ColumnsDescription columns = ColumnsDescription{sample_block.getNamesAndTypesList()};
        StoragePtr storage;

        if (useStorageMemory)
        {
            // Validate path
            if (!startsWith(absolute_path, db_data_path))
                throw Exception("Part path " + absolute_path + " is not inside " + db_data_path, ErrorCodes::DATABASE_ACCESS_DENIED);

            // Create Storage Memory
            storage = StorageMemory::create(getName(), columns);
            storage->startup();
            BlockOutputStreamPtr output = storage->write(ASTPtr(), context.getSettingsRef());

            // Write data
            std::unique_ptr<ReadBuffer> read_buffer = std::make_unique<ReadBufferFromFile>(absolute_path);
            BlockInputStreamPtr data = std::make_shared<AsynchronousBlockInputStream>(context.getInputFormat(
                    format, *read_buffer, sample_block, DEFAULT_BLOCK_SIZE));

            data->readPrefix();
            output->writePrefix();
            while(Block block = data->read())
                output->write(block);
            data->readSuffix();
            output->writeSuffix();

        }
        else
        {
            Context var_context = context;
            storage = StorageFile::create(absolute_path, -1, db_data_path, getName(), format, columns, var_context);
            storage->startup();
        }

        return storage;
    }


    void registerTableFunctionFile(TableFunctionFactory & factory)
    {
        factory.registerFunction<TableFunctionFile>();
    }

}
