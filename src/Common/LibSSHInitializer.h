#pragma once

namespace ssh
{

class LibSSHInitializer
{
public:
    LibSSHInitializer(const LibSSHInitializer &) = delete;
    LibSSHInitializer & operator=(const LibSSHInitializer &) = delete;

    static LibSSHInitializer & instance();

    ~LibSSHInitializer();

private:
    LibSSHInitializer(); // NOLINT
};

}
