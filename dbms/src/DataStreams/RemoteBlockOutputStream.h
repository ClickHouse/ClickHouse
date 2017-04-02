#pragma once

#include <Core/Block.h>
#include <DataStreams/IBlockOutputStream.h>


namespace DB
{

class Connection;
class ReadBuffer;
struct Settings;


/** Allow to execute INSERT query on remote server and send data for it.
  */
class RemoteBlockOutputStream : public IBlockOutputStream
{
public:
    RemoteBlockOutputStream(Connection & connection_, const String & query_, const Settings * settings_ = nullptr);


    /// You can call this method after 'writePrefix', to get table required structure. (You must send data with that structure).
    Block getSampleBlock() const
    {
        return sample_block;
    }

    void writePrefix() override;
    void write(const Block & block) override;
    void writeSuffix() override;

    /// Send pre-serialized and possibly pre-compressed block of data, that will be read from 'input'.
    void writePrepared(ReadBuffer & input, size_t size = 0);

private:
    Connection & connection;
    String query;
    const Settings * settings;
    Block sample_block;
};

}
