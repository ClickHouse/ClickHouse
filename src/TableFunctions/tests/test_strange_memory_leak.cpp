/// Demonstrates mysterious memory leak when built using clang version 10.0.0-4ubuntu1~18.04.2
/// Ne leaks are found by valgrind when built using gcc version 9.2.1 20191102

//#include <DataTypes/IDataType.h>
//#include <Storages/ColumnsDescription.h>

//class ITableFunction : public std::enable_shared_from_this<ITableFunction>
//{
//public:
//    virtual DB::ColumnsDescription getActualTableStructure() const = 0;
//    virtual ~ITableFunction() {}
//
//};
//
//class TableFunction : public ITableFunction
//{
//    DB::ColumnsDescription getActualTableStructure() const override
//    {
//        return DB::ColumnsDescription({{"number", DB::DataTypePtr{}}});
//    }
//};
//
//template <bool multithreaded>
//class TableFunctionTemplate : public ITableFunction
//{
//    DB::ColumnsDescription getActualTableStructure() const override
//    {
//        return DB::ColumnsDescription({{"number", DB::DataTypePtr{}}});
//    }
//};

/// Simplified version of the commented code above (without dependencies on ClickHouse code):

#include <memory>
#include <string>
#include <list>
#include <boost/multi_index_container.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

using LolPtr = std::shared_ptr<std::string>;
struct Elem
{
    Elem(std::string n, LolPtr t) : name(std::move(n)), type(std::move(t)) {}
    std::string name;
    LolPtr type;
};

using Container = boost::multi_index_container<
        Elem,
        boost::multi_index::indexed_by<
                boost::multi_index::sequenced<>,
                boost::multi_index::ordered_unique<boost::multi_index::member<Elem, std::string, &Elem::name>>>>;

struct List : public std::list<Elem>
{
    List(std::initializer_list<Elem> init) : std::list<Elem>(init) {}
};

struct Kek
{
    Container container;
    Kek(List list)
    {
        for (auto & elem : list)
            add(Elem(std::move(elem.name), std::move(elem.type)));
    }

    void add(Elem column)
    {
        auto insert_it = container.cbegin();
        container.get<0>().insert(insert_it, std::move(column));
    }

};

class ITableFunction : public std::enable_shared_from_this<ITableFunction>
{
public:
    virtual Kek getActualTableStructure() const = 0;
    virtual ~ITableFunction() {}

};

class TableFunction : public ITableFunction
{
    Kek getActualTableStructure() const override
    {
        return Kek({{"number", LolPtr{}}});
    }
};

template <bool multithreaded>
class TableFunctionTemplate : public ITableFunction
{
    Kek getActualTableStructure() const override
    {
        return Kek({{"number", LolPtr{}}});
    }
};

int main()
{
    /// Works fine
    const ITableFunction & tf1 = TableFunction{};
    tf1.getActualTableStructure();

    /// ERROR: LeakSanitizer: detected memory leaks
    /// and the same error with valgrind
    const ITableFunction & tf2 = TableFunctionTemplate<false>{};
    tf2.getActualTableStructure();
}
