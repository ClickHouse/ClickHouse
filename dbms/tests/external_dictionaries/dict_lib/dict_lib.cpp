#include <cstdint>
#include <iostream>
#include <memory>
#include <vector>

#include <Dictionaries/LibDictionarySourceExternal.h>

#define DUMPS(VAR) #VAR " = " << VAR
#define DUMP(VAR) std::cerr << __FILE__ << ":" << __LINE__ << " " << DUMPS(VAR) << "\n";


//#include <common/iostream_debug_helpers.h>

/*
struct ClickhouseVectorUint64
{
    uint64_t size = 0;
    uint64_t * data = nullptr;
};

struct ClickhouseColumnsUint64
{
    uint64_t size = 0;
    ClickhouseVectorUint64 * columns = nullptr;
};
*/
//using ClickhouseColumnName = const char *;
//using ClickhouseColumnNames = ClickhouseColumnName[];

struct DataHolder
{
    //std::shared_ptr<std::vector<std::vector<uint64_t>>> vector;
    std::vector<std::vector<uint64_t>> vector;
    std::unique_ptr<ClickhouseVectorUint64[]> columnsHolder;
    ClickhouseColumnsUint64 columns;
};

extern "C" void * loadIds(void * data_ptr, ClickhouseStrings * columns, const struct ClickhouseVectorUint64 * ids)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);
    std::cerr << "loadIds Runned!!! ptr=" << data_ptr << " => " << ptr << " size=" << ids->size << "\n";
    if (columns)
    {
        std::cerr << "columns passed:" << columns->size << "\n";
        for (size_t i = 0; i < columns->size; ++i)
        {
            std::cerr << "col " << i << " :" << columns->data[i] << "\n";
        }
    }
    if (ids)
    {
        std::cerr << "ids passed: " << ids->size << "\n";
        for (size_t i = 0; i < ids->size; ++i)
        {
            std::cerr << "id " << i << " :" << ids->data[i] << "\n";
        }
    }
    if (ptr)
    {
        //ptr->vector.resize(columns->size + 1); // columns + id
        //ptr->vector.assign({{1, 2, 3, 4, 5, 6}, {11, 12, 13, 14}, {21, 22, 23, 24, 25}});
        //DUMP(ptr->vector);
        //std::make_unique<const char * []>(key_columns.size() + 1);

        //ptr->vector[0] = {};
        /*
        ptr->vector.assign({
            {0, 50, 13874, 980694578, 980694579, 50, 13874, 980694578, 980694579, 0, 0, 4761183170873013810, 0, 0, 0},
            {1, 48,                57392,                4083802160,                4083802161,                48,                -8144,                -211165136,                -211165135,                1.5,                1.5,                10577349846663553072,                20370602,                19700102095024,                0},
            {2,                69,                35909,                1447922757,                1447922758,                69,                -29627,                1447922757,                1447922758,                3,                3,                18198135717204167749,                19780808,                19700102035221,                1},
            {3,                250,                1274,                1029309690,                1029309691,                -6,                1274,                1029309690,                1029309691,                4.5,                4.5,                9624464864560415994,                19730628,                19700102032114,                2}});
            */

        ptr->vector.assign({
            {0, 50, 13874, 980694578, 980694579, 50, 13874, 980694578, 980694579, 0, 0, 4761183170873, 0, 0, 0},
            {1, 48,                57392,                4083802160,                4083802161,                48,                8144,                211165136,                211165135,                15,                15,                                105773498466635,                20370602,                19700102095024,                0},
            {2,                69,                35909,                1447922757,                1447922758,                69,                29627,                1447922757,                1447922758,                3,                3,                  18198135717204,                19780808,                1970010203521,                1},
            {3,                250,                1274,                1029309690,                1029309691,                6,                1274,                1029309690,                1029309691,                45,                45,                96244648645604,                19730628,                19700102032114,                2}});

        /*
         {0,	50,	13874,	980694578,	980694579,	50,	13874,	980694578,	980694579,	0,	0,	4761183170873013810,	"2007-12-27",	"1970-01-02 06:51:14", 0},
         {1,	48,	57392,	4083802160,	4083802161,	48,	-8144,	-211165136,	-211165135,	1.5,	1.5,	10577349846663553072	"2037-06-02",	"1970-01-02 09:50:24",	0},
         {2,	69,	35909,	1447922757,	1447922758,	69,	-29627,	1447922757,	1447922758,	3,	3	18198135717204167749,	"1978-08-08"	"1970-01-02 03:52:21",	1},
         {3,	250,1274,	1029309690,	1029309691,	-6,	1274,	1029309690,	1029309691,	4.5,	4.5,	9624464864560415994,	"1973-06-28",	"1970-01-02 03:21:14",	2},
         */


        ptr->columnsHolder = std::make_unique<ClickhouseVectorUint64[]>(ptr->vector.size());
        size_t i = 0;
        for (auto & col : ptr->vector)
        {
            DUMP(i);
            //DUMP(col);
            //col.resize();
            ptr->columnsHolder[i].size = col.size();
            ptr->columnsHolder[i].data = col.data();
            ++i;
        }
        ptr->columns.size = ptr->vector.size();
        DUMP(ptr->columns.size);
        ptr->columns.data = ptr->columnsHolder.get();
        //DUMP(ptr->columns.columns);
        return static_cast<void *>(&ptr->columns);
    }

    // {ptr->size(), ptr->data()};
    return nullptr;
}

extern "C" void * loadAll(void * data_ptr, ClickhouseStrings * columns)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);
    std::cerr << "loadAll Runned!!! ptr=" << data_ptr << " => " << ptr << "\n";
    if (ptr)
    {
        return static_cast<void *>(&ptr->columns);
    }
    //return;
    return nullptr;
}

/*
extern "C" void loadKeys(void * data_ptr, ClickhouseColumns columns, const struct ClickhouseVectorUint64 requested_rows)
{
    std::cerr << "loadKeys Runned!!! ptr=" << data_ptr << " size=" << requested_rows.size << "\n";
    size_t i = 0;
    //auto column = columns[i];
    ClickhouseColumn column;
    while ((column = columns[i++]))
    {
        std::cerr << "column i=" << i << " = [" << column << "] p=" << (size_t)column << "\n";
    }
    return;
}
*/

extern "C" void * dataAllocate()
{
    //int size = 100;
    //auto data_ptr = ::operator new(size);
    auto data_ptr = new DataHolder;

    //auto ptr = static_cast<DataHolder*>(data_ptr);

    std::cerr << "dataAllocate Runned!!! ptr=" << data_ptr << "\n";
    return data_ptr;
}

extern "C" void dataDelete(void * data_ptr)
{
    auto ptr = static_cast<DataHolder *>(data_ptr);
    std::cerr << "dataDelete Runned!!! ptr=" << data_ptr << " => " << ptr << "\n";
    delete ptr;
    return;
}
