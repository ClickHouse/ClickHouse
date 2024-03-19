#pragma once

#include <TableFunctions/ITableFunctionFileLike.h>


namespace DB {
    class Context;

/* sftp(URI, [format, structure, compression]) - creates a temporary storage from hdfs files
 *
 */
    class TableFunctionSFTP : public ITableFunctionFileLike {
    public:
        static constexpr auto name = "sftp";
        static constexpr auto signature = " - host, path, user, password | DAEMON_AUTH\n"
                                          " - host, path, user, password | DAEMON_AUTH, format\n"
                                          " - host, path, user, password | DAEMON_AUTH, format, structure\n"
                                          " - host, path, user, password | DAEMON_AUTH, format, structure, compression_method\n"
                                          " - host, path, user, password | DAEMON_AUTH, port\n"
                                          " - host, path, user, password | DAEMON_AUTH, port, format\n"
                                          " - host, path, user, password | DAEMON_AUTH, port, format, structure\n"
                                          " - host, path, user, password | DAEMON_AUTH, port, format, structure, compression_method\n";

        String getName() const override {
            return name;
        }

        String getSignature() const override {
            return signature;
        }

        ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

        std::unordered_set<String> getVirtualsToCheckBeforeUsingStructureHint() const override {
            return {"_path", "_file"};
        }

        void parseArgumentsImpl(ASTs &args, const ContextPtr &context) override;

    private:
        StoragePtr getStorage(
                const String &source, const String &format_, const ColumnsDescription &columns,
                ContextPtr global_context,
                const std::string &table_name, const String &compression_method_) const override;

        const char *getStorageTypeName() const override { return "SFTP"; }

        StorageSFTP::Configuration configuration;
    };

}
