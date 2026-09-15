#pragma once
#include <base/types.h>
#include <utility>

namespace DB::Cas
{

class Layout;

/// The key of an object that is written once and never rewritten: a part manifest, a ref log, a ref
/// snapshot. Only `Layout` mints one, from the typed identity of such an object, so a verb that
/// accepts this type can delete without a precondition: whatever body the key holds is the one body
/// it ever held. A mutable control object (a checkpoint, the catalog, `gc/state`, a mount lease) has
/// no path to this type.
class WriteOnceKey
{
public:
    const String & str() const { return key; }

private:
    friend class Layout;
    explicit WriteOnceKey(String key_) : key(std::move(key_)) {}
    String key;
};

}
