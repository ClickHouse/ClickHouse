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
    };

    struct Object
    {
        const void * address_begin;
        const void * address_end;
        std::string name;
    };

    SymbolIndex() { update(); }
    void update();

    const Symbol * findSymbol(const void * address) const;
    const Object * findObject(const void * address) const;

    const std::vector<Symbol> & symbols() const { return data.symbols; }
    const std::vector<Object> & objects() const { return data.objects; }

    struct Data
    {
        std::vector<Symbol> symbols;
        std::vector<Object> objects;
    };
private:
    Data data;
};

}
