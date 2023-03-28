#include <google/protobuf/compiler/importer.h>

struct ErrorCollector : public google::protobuf::compiler::MultiFileErrorCollector
{
    void AddError(const std::string&, int, int, const std::string& msg) override
    {
        throw std::runtime_error(msg);
    }
};

int main()
{
    google::protobuf::compiler::DiskSourceTree disk_source_tree;
    disk_source_tree.MapPath("", "schemas/");
    ErrorCollector collector;
    google::protobuf::compiler::Importer importer(&disk_source_tree, &collector);
    try
    {
        importer.Import("schema.proto");
    }
    catch()
    {
    }

    importer.pool()->FindMessageTypeByName("Schema");
}
