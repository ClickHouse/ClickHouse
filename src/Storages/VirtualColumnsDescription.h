#pragma once
#include <Storages/ColumnsDescription.h>

namespace DB
{

struct VirtualColumnDescription : public ColumnDescription
{
public:
    using Self = VirtualColumnDescription;
    VirtualsKind kind;

    VirtualColumnDescription() = default;
    VirtualColumnDescription(String name_, DataTypePtr type_, ASTPtr codec_, String comment_, VirtualsKind kind_);

    bool isEphemeral() const { return kind == VirtualsKind::Ephemeral; }
    bool isPersistent() const { return kind == VirtualsKind::Persistent; }

    struct Comparator
    {
        using is_transparent = void;
        bool operator()(const Self & lhs, const Self & rhs) const { return lhs.name < rhs.name; }
        bool operator()(const Self & lhs, const String & rhs) const { return lhs.name < rhs; }
        bool operator()(const String & lhs, const Self & rhs) const { return lhs < rhs.name; }
    };
};

class VirtualColumnsDescription
{
public:
    using Container = std::set<VirtualColumnDescription, VirtualColumnDescription::Comparator>;
    using const_iterator = Container::const_iterator;

    const_iterator begin() const { return container.begin(); }
    const_iterator end() const { return container.end(); }

    VirtualColumnsDescription() = default;

    void add(VirtualColumnDescription desc);
    void addEphemeral(String name, DataTypePtr type, String comment);
    void addPersistent(String name, DataTypePtr type, ASTPtr codec, String comment);

    bool empty() const { return container.empty(); }
    bool has(const String & name) const { return container.contains(name); }

    NameAndTypePair get(const String & name, VirtualsKind kind) const;
    std::optional<NameAndTypePair> tryGet(const String & name, VirtualsKind kind) const;

    NameAndTypePair get(const String & name) const { return get(name, VirtualsKind::All); }
    std::optional<NameAndTypePair> tryGet(const String & name) const { return tryGet(name, VirtualsKind::All); }

    std::optional<VirtualColumnDescription> tryGetDescription(const String & name, VirtualsKind kind) const;
    VirtualColumnDescription getDescription(const String & name, VirtualsKind kind) const;

    std::optional<VirtualColumnDescription> tryGetDescription(const String & name) const { return tryGetDescription(name, VirtualsKind::All); }
    VirtualColumnDescription getDescription(const String & name) const { return getDescription(name, VirtualsKind::All); }

    NamesAndTypesList get(VirtualsKind kind) const;
    NamesAndTypesList getNamesAndTypesList() const;

    Block getSampleBlock() const;
    Block getSampleBlock(const Names & names) const;

private:
    Container container;
};

}
