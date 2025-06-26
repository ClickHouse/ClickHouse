#pragma once

#include <Poco/Util/Application.h>
#include <Common/ZooKeeper/KeeperClientCLI/KeeperClient.h>

namespace DB
{

class KeeperClient: public Poco::Util::Application, public KeeperClientBase
{
public:
    void initialize(Poco::Util::Application & self) override;

    int main(const std::vector<String> & args) override;

    void defineOptions(Poco::Util::OptionSet & options) override;

    String executeFourLetterCommand(const String & command) final;

protected:
    void runInteractive();
    void runInteractiveReplxx();
    void runInteractiveInputStream();
    std::vector<String> getCompletions(const String & prefix) const;

    zkutil::ZooKeeperArgs zk_args;

    String history_file;
    UInt32 history_max_entries = 0; /// Maximum number of entries in the history file.

    LineReader::Suggest suggest;
};

}
