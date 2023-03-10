#pragma once


#include <Common/ZooKeeper/ZooKeeper.h>
#include <Client/LineReader.h>
#include <Poco/Util/Application.h>
#include <filesystem>


namespace DB
{

class KeeperClient;

class KeeperClient: public Poco::Util::Application
{
public:
    using Callback = std::function<void(KeeperClient *, const std::vector<String> &)>;

    KeeperClient() = default;

    void initialize(Poco::Util::Application & self) override;

    int main(const std::vector<String> & args) override;

    void defineOptions(Poco::Util::OptionSet & options) override;

protected:
    void runInteractive();
    void loadCommands(std::vector<std::tuple<String, size_t, Callback>> &&);
    bool processQueryText(const String & text);

    String getAbsolutePath(const String & relative);

    std::map<std::pair<String, size_t>, Callback> commands;

    String history_file;
    LineReader::Suggest suggest;

    zkutil::ZooKeeperPtr zookeeper;
    std::filesystem::path cwd = "/";
};

}
