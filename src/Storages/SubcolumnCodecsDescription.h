#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>

#include <map>
#include <memory>

namespace DB
{

/// Codecs declared for the subcolumns of a column, by the name of the subcolumn relative to the column (`meta.seq`).
class SubcolumnCodecsDescription
{
public:
    SubcolumnCodecsDescription() = default;
    SubcolumnCodecsDescription(const SubcolumnCodecsDescription & other);
    SubcolumnCodecsDescription & operator=(const SubcolumnCodecsDescription & other);
    SubcolumnCodecsDescription(SubcolumnCodecsDescription &&) noexcept = default;
    SubcolumnCodecsDescription & operator=(SubcolumnCodecsDescription &&) noexcept = default;

    bool empty() const { return !codecs || codecs->empty(); }
    bool contains(const String & subcolumn_name) const { return codecs && codecs->contains(subcolumn_name); }

    /// Returns the codec declared for the subcolumn, or nullptr.
    ASTPtr get(const String & subcolumn_name) const;

    /// Declares the codec for the subcolumn, replacing a declared one.
    void set(const String & subcolumn_name, ASTPtr codec);

    /// Removes the declaration for the subcolumn, if there is one.
    void erase(const String & subcolumn_name);
    void clear() { codecs.reset(); }

    /// Compares the names and the formatted codecs.
    bool operator==(const SubcolumnCodecsDescription & other) const;

    /// Iterates in the order of the names, like the map.
    using Map = std::map<String, ASTPtr>;
    Map::const_iterator begin() const { return map().begin(); }
    Map::const_iterator end() const { return map().end(); }
    Map::iterator begin() { return mutableMap().begin(); }
    Map::iterator end() { return mutableMap().end(); }

private:
    const Map & map() const;
    Map & mutableMap();

    /// The map is allocated only when a codec is set.
    std::unique_ptr<Map> codecs;
};

}
