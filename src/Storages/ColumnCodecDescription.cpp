#include <Storages/ColumnCodecDescription.h>

#include <Common/Exception.h>
#include <Parsers/IAST.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

ColumnCodecDescription::ColumnCodecDescription(const ColumnCodecDescription & other)
{
    *this = other;
}

ColumnCodecDescription & ColumnCodecDescription::operator=(const ColumnCodecDescription & other)
{
    if (this == &other)
        return *this;
    codecs.clear();
    for (const auto & [path, codec] : other.codecs)
        codecs.emplace(path, codec->clone());
    return *this;
}

const ASTPtr & ColumnCodecDescription::getRoot() const
{
    static const ASTPtr null_codec;
    auto it = codecs.find(CodecPath{});
    return it == codecs.end() ? null_codec : it->second;
}

void ColumnCodecDescription::setRoot(const ASTPtr & ast)
{
    if (ast)
        codecs[CodecPath{}] = ast->clone();
    else
        resetRoot();
}

void ColumnCodecDescription::set(CodecPath path, const ASTPtr & ast)
{
    if (!ast)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A codec declaration requires a codec expression");
    codecs[std::move(path)] = ast->clone();
}

void ColumnCodecDescription::erase(const CodecPath & path)
{
    codecs.erase(path);
}

ColumnCodecDescription::Declaration ColumnCodecDescription::find(const CodecPath & logical_path) const
{
    CodecPath candidate = logical_path;
    while (true)
    {
        if (auto it = codecs.find(candidate); it != codecs.end())
        {
            return {it->second, candidate};
        }
        if (candidate.empty())
            break;
        candidate.pop_back();
    }
    return {};
}

bool ColumnCodecDescription::operator==(const ColumnCodecDescription & rhs) const
{
    auto format = [](const ASTPtr & ast) { return ast ? ast->formatWithSecretsOneLine() : String{}; };
    if (codecs.size() != rhs.codecs.size())
        return false;
    auto lhs_it = codecs.begin();
    auto rhs_it = rhs.codecs.begin();
    for (; lhs_it != codecs.end(); ++lhs_it, ++rhs_it)
        if (lhs_it->first != rhs_it->first || format(lhs_it->second) != format(rhs_it->second))
            return false;
    return true;
}

}
