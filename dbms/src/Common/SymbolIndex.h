#pragma once

#include <vector>
#include <string>


namespace DB
{

/** Allow to quickly find symbol name from address.
  * Used as a replacement for "dladdr" function which is extremely slow.
  */
class SymbolIndex
{
public:
    struct Symbol
    {
        const void * address_begin;
        const void * address_end;
        const char * object;
        std::string name;   /// demangled NOTE Can use Arena for strings

        bool operator< (const Symbol & rhs) const { return address_begin < rhs.address_begin; }
        bool operator< (const void * addr) const { return address_begin <= addr; }
    };

    SymbolIndex() { update(); }
    void update();

    const Symbol * find(const void * address) const;

    auto begin() const { return symbols.cbegin(); }
    auto end() const { return symbols.cend(); }

private:
    std::vector<Symbol> symbols;
};

}
