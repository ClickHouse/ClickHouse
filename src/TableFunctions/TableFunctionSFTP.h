//#pragma once
//
//#include "config.h"
//
//#include <TableFunctions/ITableFunctionFileLike.h>
//
//
//namespace DB
//{
//
//    class Context;
//
///* sftp(URI, [format, structure, compression]) - creates a temporary storage from hdfs files
// *
// */
//    class TableFunctionSFTP : public ITableFunctionFileLike
//    {
//    public:
//        static constexpr auto name = "sftp";
//        static constexpr auto signature = " - uri\n"
//                                          " - uri, format\n"
//                                          " - uri, format, structure\n"
//                                          " - uri, format, structure, compression_method\n";
//
//        String getName() const override
//        {
//            return name;
//        }
//
//        String getSignature() const override
//        {
//            return signature;
//        }
//
//        ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
//
//        std::unordered_set<String> getVirtualsToCheckBeforeUsingStructureHint() const override
//        {
//            return {"_path", "_file"};
//        }
//
//    private:
//        StoragePtr getStorage(
//                const String & source, const String & format_, const ColumnsDescription & columns, ContextPtr global_context,
//                const std::string & table_name, const String & compression_method_) const override;
//        const char * getStorageTypeName() const override { return "SFTP"; }
//    };
//
//}
