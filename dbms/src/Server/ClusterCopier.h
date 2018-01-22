#pragma once
#include <Poco/Util/ServerApplication.h>

namespace DB
{

class ClusterCopierApp : public Poco::Util::ServerApplication
{
public:

    void initialize(Poco::Util::Application & self) override;

    void handleHelp(const std::string &, const std::string &);

    void defineOptions(Poco::Util::OptionSet & options) override;

    int main(const std::vector<std::string> &) override;

private:

    void mainImpl();

    void setupLogging();

    std::string config_xml_path;
    std::string task_path;
    std::string log_level = "debug";
    bool is_safe_mode = false;
    double copy_fault_probability = 0;
    bool is_help = false;

    std::string base_dir;
    std::string process_path;
    std::string process_id;
    std::string host_id;
};

}
