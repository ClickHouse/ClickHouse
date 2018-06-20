#pragma once

#include <memory>
#include <functional>
#include <unordered_map>
#include <ext/singleton.h>
#include <Core/Types.h>


namespace DB
{

class Block;
class Context;
struct FormatSettings;

class ReadBuffer;
class WriteBuffer;

class IBlockInputStream;
class IBlockOutputStream;

using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;


/** Allows to create an IBlockInputStream or IBlockOutputStream by the name of the format.
  * Note: format and compression are independent things.
  */
class FormatFactory final : public ext::singleton<FormatFactory>
{
private:
    using InputCreator = std::function<BlockInputStreamPtr(
        ReadBuffer & buf,
        const Block & sample,
        const Context & context,
        size_t max_block_size,
        const FormatSettings & settings)>;

    using OutputCreator = std::function<BlockOutputStreamPtr(
        WriteBuffer & buf,
        const Block & sample,
        const Context & context,
        const FormatSettings & settings)>;

    using Creators = std::pair<InputCreator, OutputCreator>;

    using FormatsDictionary = std::unordered_map<String, Creators>;

public:
    BlockInputStreamPtr getInput(const String & name, ReadBuffer & buf,
        const Block & sample, const Context & context, size_t max_block_size) const;

    BlockOutputStreamPtr getOutput(const String & name, WriteBuffer & buf,
        const Block & sample, const Context & context) const;

    /// Register format by its name.
    void registerInputFormat(const String & name, InputCreator input_creator);
    void registerOutputFormat(const String & name, OutputCreator output_creator);

private:
    FormatsDictionary dict;

    FormatFactory();
    friend class ext::singleton<FormatFactory>;

    const Creators & getCreators(const String & name) const;
};

}
