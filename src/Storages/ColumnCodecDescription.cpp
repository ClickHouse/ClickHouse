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

    if (other.empty())
    {
        codecs.reset();
        return *this;
    }

    auto new_codecs = std::make_unique<CodecsByPath>();
    for (const auto & [path, codec] : other.getCodecs())
        new_codecs->emplace(path, codec->clone());
    codecs = std::move(new_codecs);
    return *this;
}

const ColumnCodecDescription::CodecsByPath & ColumnCodecDescription::getCodecs() const
{
    static const CodecsByPath empty_codecs;
    return codecs ? *codecs : empty_codecs;
}

const ASTPtr & ColumnCodecDescription::getRoot() const
{
    static const ASTPtr null_codec;
    const auto & declarations = getCodecs();
    auto it = declarations.find(CodecPath{});
    return it == declarations.end() ? null_codec : it->second;
}

void ColumnCodecDescription::setRoot(const ASTPtr & ast)
{
    if (ast)
    {
        auto cloned_ast = ast->clone();
        if (!codecs)
        {
            auto new_codecs = std::make_unique<CodecsByPath>();
            new_codecs->emplace(CodecPath{}, std::move(cloned_ast));
            codecs = std::move(new_codecs);
        }
        else
            codecs->insert_or_assign(CodecPath{}, std::move(cloned_ast));
    }
    else
        resetRoot();
}

void ColumnCodecDescription::resetRoot()
{
    erase(CodecPath{});
}

void ColumnCodecDescription::set(CodecPath path, const ASTPtr & ast)
{
    if (!ast)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A codec declaration requires a codec expression");

    auto cloned_ast = ast->clone();
    if (!codecs)
    {
        auto new_codecs = std::make_unique<CodecsByPath>();
        new_codecs->emplace(std::move(path), std::move(cloned_ast));
        codecs = std::move(new_codecs);
    }
    else
        codecs->insert_or_assign(std::move(path), std::move(cloned_ast));
}

void ColumnCodecDescription::erase(const CodecPath & path)
{
    if (!codecs)
        return;

    codecs->erase(path);
    if (codecs->empty())
        codecs.reset();
}

ColumnCodecDescription::Declaration ColumnCodecDescription::find(const CodecPath & logical_path) const
{
    const auto & declarations = getCodecs();
    CodecPath candidate = logical_path;
    while (true)
    {
        if (auto it = declarations.find(candidate); it != declarations.end())
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
    const auto & lhs_codecs = getCodecs();
    const auto & rhs_codecs = rhs.getCodecs();
    if (lhs_codecs.size() != rhs_codecs.size())
        return false;
    auto lhs_it = lhs_codecs.begin();
    auto rhs_it = rhs_codecs.begin();
    for (; lhs_it != lhs_codecs.end(); ++lhs_it, ++rhs_it)
        if (lhs_it->first != rhs_it->first || format(lhs_it->second) != format(rhs_it->second))
            return false;
    return true;
}

}
