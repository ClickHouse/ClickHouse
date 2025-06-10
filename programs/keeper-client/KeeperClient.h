#pragma once

#include <Poco/Util/Application.h>
#include <Common/ZooKeeper/KeeperClientCLI/KeeperClient.h>


class KeeperClient: public Poco::Util::Application, public KeeperClientBase
{
public:
    void initialize(Poco::Util::Application & self) override;

    int main(const std::vector<String> & args) override;

    void defineOptions(Poco::Util::OptionSet & options) override;

protected:
    void runInteractive();
    void runInteractiveReplxx();
    void runInteractiveInputStream();
};

}
