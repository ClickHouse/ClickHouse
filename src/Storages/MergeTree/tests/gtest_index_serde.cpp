#include <gtest/gtest.h>

#include <utility>
#include <limits>
#include <set>

#include <Storages/MergeTree/MergeTreeIndexDiskANN.h>
#include "Columns/ColumnsNumber.h"
#include "Columns/IColumn.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "IO/ReadBufferFromFileBase.h"
#include <DataTypes/DataTypesNumber.h>


namespace DB {

namespace test_detail {

void saveDataPoints(uint32_t dimensions, std::vector<DiskANNValue> datapoints, WriteBuffer & out) {
    uint32_t num_of_points = static_cast<uint32_t>(datapoints.size()) / dimensions;

    out.write(reinterpret_cast<const char*>(&num_of_points), sizeof(num_of_points));
    out.write(reinterpret_cast<const char*>(&dimensions), sizeof(dimensions));

    for (float data_point : datapoints) {
        out.write(reinterpret_cast<const char*>(&data_point), sizeof(Float32));
    }

}

DiskANNIndexPtr constructIndexFromDatapoints(uint32_t dimensions, std::vector<DiskANNValue> datapoints) {
    if (datapoints.empty()) {
        throw Exception("Trying to construct index with no datapoints", ErrorCodes::LOGICAL_ERROR);
    }

    String datapoints_filename = "diskann_datapoints.bin";
    WriteBufferFromFile write_buffer(datapoints_filename);
    saveDataPoints(dimensions, datapoints, write_buffer);
    write_buffer.close();

    return std::make_shared<DiskANNIndex>(
        diskann::Metric::L2,
        datapoints_filename.c_str()
    );
}

MergeTreeIndexGranuleDiskANN constructTestGranule(uint32_t dim, std::vector<DiskANNValue> datapoints, bool empty=false) {    
    auto column = ColumnFloat32::create();
    for (auto f : datapoints) {
        column->insert(f);
    }

    column->insertData(reinterpret_cast<const char*>(datapoints.data()), datapoints.size());

    std::cerr << "Created column and inserted test data" << std::endl;

    auto column_with_type_name = ColumnWithTypeAndName(
        std::move(column), 
        std::make_shared<DataTypeFloat32>(),
        "number"
    );

    std::cerr << "created column with name" << std::endl;   

    Block sample_block{column_with_type_name};
    
    std::cerr << "created sample block" << std::endl;

    if (empty) {
        return MergeTreeIndexGranuleDiskANN("Diskann", sample_block);
    } else {
        auto base_index = test_detail::constructIndexFromDatapoints(dim, datapoints);
        
        diskann::Parameters paras;
        paras.Set<unsigned>("R", 100);
        paras.Set<unsigned>("L", 150);
        paras.Set<unsigned>("C", 750);
        paras.Set<float>("alpha", 1.2);
        paras.Set<bool>("saturate_graph", true);
        paras.Set<unsigned>("num_threads", 1);

        base_index->build(paras);

        return MergeTreeIndexGranuleDiskANN("Diskann", sample_block, base_index, dim, datapoints);
    }
}

}


TEST(IndexSerde, DiskANN)
{
    uint32_t dim = 1;
    std::vector<DiskANNValue> datapoints = {
        0.f, 1.f, 2.f, 3.f, 4.f
    };

    std::cerr << "Constructing test granule" << std::endl;
    auto test_granule = test_detail::constructTestGranule(dim, datapoints, false);
    std::cerr << "Constructed sample granule" << std::endl;

    String index_ser_file = "index.bin";
    WriteBufferFromFile write_buffer(index_ser_file);

    test_granule.serializeBinary(write_buffer);
    write_buffer.close();

    auto deserialized_granule = test_detail::constructTestGranule(dim, datapoints, true);
    ReadBufferFromFile read_buffer(index_ser_file);
    deserialized_granule.deserializeBinary(read_buffer, 0);
    read_buffer.close();

    ASSERT_TRUE(test_granule.dimensions.has_value());
    ASSERT_TRUE(deserialized_granule.dimensions.has_value());
    ASSERT_EQ(test_granule.dimensions.value(), deserialized_granule.dimensions.value());
    ASSERT_EQ(test_granule.datapoints.size(), deserialized_granule.datapoints.size());

    for (size_t i = 0; i < test_granule.datapoints.size(); ++i) {
        ASSERT_EQ(test_granule.datapoints[i], deserialized_granule.datapoints[i]); 
    }
}

}

