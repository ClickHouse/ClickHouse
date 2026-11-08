#include <Storages/SubcolumnCodecsDescription.h>

#include <Parsers/IAST.h>

#include <algorithm>


namespace DB
{

SubcolumnCodecsDescription::SubcolumnCodecsDescription(const SubcolumnCodecsDescription & other)
{
    *this = other;
}

SubcolumnCodecsDescription & SubcolumnCodecsDescription::operator=(const SubcolumnCodecsDescription & other)
{
    if (this == &other)
        return *this;

    codecs.reset();
    for (const auto & [subcolumn_name, codec] : other)
        set(subcolumn_name, codec->clone());
    return *this;
}

ASTPtr SubcolumnCodecsDescription::get(const String & subcolumn_name) const
{
    if (!codecs)
        return nullptr;
    auto it = codecs->find(subcolumn_name);
    return it == codecs->end() ? nullptr : it->second;
}

void SubcolumnCodecsDescription::set(const String & subcolumn_name, ASTPtr codec)
{
    mutableMap()[subcolumn_name] = std::move(codec);
}

void SubcolumnCodecsDescription::erase(const String & subcolumn_name)
{
    if (!codecs)
        return;
    codecs->erase(subcolumn_name);
    if (codecs->empty())
        codecs.reset();
}

bool SubcolumnCodecsDescription::operator==(const SubcolumnCodecsDescription & other) const
{
    return std::ranges::equal(*this, other, [](const auto & left, const auto & right)
    {
        return left.first == right.first && left.second->formatWithSecretsOneLine() == right.second->formatWithSecretsOneLine();
    });
}

const SubcolumnCodecsDescription::Map & SubcolumnCodecsDescription::map() const
{
    static const Map empty_map;
    return codecs ? *codecs : empty_map;
}

SubcolumnCodecsDescription::Map & SubcolumnCodecsDescription::mutableMap()
{
    if (!codecs)
        codecs = std::make_unique<Map>();
    return *codecs;
}

}
