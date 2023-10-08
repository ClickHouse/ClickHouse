//#include "config.h"
//#include "registerTableFunctions.h"
//
//#include <Storages/SFTP/StorageSFTP.h>
//#include <Storages/ColumnsDescription.h>
//#include <TableFunctions/TableFunctionFactory.h>
//#include <TableFunctions/TableFunctionSFTP.h>
//#include <Interpreters/parseColumnsListForTableFunction.h>
//#include <Interpreters/Context.h>
//#include <Access/Common/AccessFlags.h>
//
//namespace DB
//{
//
//    StoragePtr TableFunctionSFTP::getStorage(
//            const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
//            const std::string & table_name, const String & compression_method_) const
//    {
//        return std::make_shared<StorageSFTP>(
//                source,
//                StorageID(getDatabaseName(), table_name),
//                format_,
//                columns,
//                ConstraintsDescription{},
//                String{},
//                global_context,
//                compression_method_);
//    }
//
//    ColumnsDescription TableFunctionSFTP::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
//    {
//        if (structure == "auto")
//        {
//            context->checkAccess(getSourceAccessType());
//            return StorageSFTP::getTableStructureFromData(format, filename, compression_method, context);
//        }
//
//        return parseColumnsListFromString(structure, context);
//    }
//
//    void registerTableFunctionHDFS(TableFunctionFactory & factory)
//    {
//        factory.registerFunction<TableFunctionSFTP>();
//    }
//
//}
