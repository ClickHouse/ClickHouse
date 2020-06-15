#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMergeTree.h>
#include <DataStreams/copyData.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteBufferFromOStream.h>
#include <Interpreters/Context.h>
#include <Poco/Util/Application.h>
#include <Common/Config/ConfigProcessor.h>

#include <Processors/Executors/TreeExecutorBlockInputStream.h>

DB::StoragePtr createStorage(DB::Context & context)
{
    using namespace DB;

    NamesAndTypesList names_and_types;
    names_and_types.emplace_back("a", std::make_shared<DataTypeUInt64>());
    names_and_types.emplace_back("date", std::make_shared<DataTypeDateTime>());

    ColumnsDescription desc = ColumnsDescription{names_and_types};

    StorageInMemoryMetadata meta = StorageInMemoryMetadata(desc, IndicesDescription{}, ConstraintsDescription{});
    meta.order_by_ast = std::make_shared<ASTIdentifier>("a");
    //meta.partition_by_ast = std::make_shared<ASTIdentifier>("date");
    //auto context_holder = getContext();
    /*try
    {
        context.getMergeTreeSettings();
    } catch (const Exception & e) {
        std::cerr << e.getStackTraceString();
    } catch (...) {
        std::cerr << "Very bad\n";
    }*/
    std::unique_ptr<MergeTreeSettings> ptr = std::make_unique<MergeTreeSettings>(context.getMergeTreeSettings());

    StoragePtr table = StorageMergeTree::create(
        StorageID("test", "test"), "table/", meta, false, context, "date", MergeTreeData::MergingParams{}, std::move(ptr), false);

    table->startup();

    return table;
}

std::string writeData(/*size_t rows,*/ DB::StoragePtr & table/*, DB::Context & context*/)
{
    using namespace DB;

    std::string data;

    Block block;

    {
        /*const auto & storage_columns =*/ table->getColumns();
        /*ColumnWithTypeAndName column1;
        column1.name = "a";
        column1.type = storage_columns.getPhysical("a").type;
        auto col1 = column1.type->createColumn();
        ColumnWithTypeAndName column2;
        column2.name = "date";
        column2.type = storage_columns.getPhysical("date").type;
        auto col2 = column2.type->createColumn();*/
        //ColumnUInt64::Container & vec = typeid_cast<ColumnUInt64 &>(*col1).getData();

        /*vec.resize(rows);
        for (size_t i = 0; i < rows; ++i)
        {
            vec[i] = i;
            if (i > 0)
                data += ",";
            data += "(" + std::to_string(i) + ")";
            column2.type->insertDefaultInto(*col2);
        }

        column1.column = std::move(col1);
        column2.column = std::move(col2);
        block.insert(column1);
        block.insert(column2);*/
    }

    //BlockOutputStreamPtr out = table->write({}, context);
    //out->write(block);

    return data;
}

std::string readData(DB::StoragePtr & table, DB::Context & context) {
    using namespace DB;

    Names column_names;
    column_names.push_back("a");
    column_names.push_back("date");

    QueryProcessingStage::Enum stage = table->getQueryProcessingStage(context);

    BlockInputStreamPtr in = std::make_shared<TreeExecutorBlockInputStream>(std::move(table->read(column_names, {}, context, stage, 8192, 1)[0]));

    Block sample;
    {
        ColumnWithTypeAndName col1, col2;
        col1.type = std::make_shared<DataTypeUInt64>();
        col2.type = std::make_shared<DataTypeDateTime>();
        sample.insert(std::move(col1));
        sample.insert(std::move(col2));
    }

    std::ostringstream ss;
    WriteBufferFromOStream out_buf(ss);
    BlockOutputStreamPtr output = FormatFactory::instance().getOutput("Values", out_buf, sample, context);

    copyData(*in, *output);

    output->flush();

    return ss.str();
}

static const char * minimal_default_user_xml =
"<yandex>"
"    <profiles>"
"        <default></default>"
"    </profiles>"
"    <users>"
"        <default>"
"            <password></password>"
"            <networks>"
"                <ip>::/0</ip>"
"            </networks>"
"            <profile>default</profile>"
"            <quota>default</quota>"
"        </default>"
"    </users>"
"    <quotas>"
"        <default></default>"
"    </quotas>"
"</yandex>";

static DB::ConfigurationPtr getConfigurationFromXMLString(const char * xml_data)
{
    std::stringstream ss{std::string{xml_data}};
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}

TEST(GrabOldModifiedParts, SimpleCase) {
    using namespace DB;
    const auto & context_holder = getContext();
    Context ctx = context_holder.context;

    ConfigurationPtr config = getConfigurationFromXMLString(minimal_default_user_xml);
    ctx.setConfig(config);

    auto table = createStorage(ctx);

    std::string data;

    //data += writeData(/*10,*/ table/*, ctx*/);
    /*data += ",";
    data += writeData(20, table, ctx);
    data += ",";
    data += writeData(10, table, ctx);*/

    //StorageMergeTree *tree = dynamic_cast<StorageMergeTree *>(table.get());
    //tree->grabOldModifiedParts();

    //ASSERT_EQ(data, readData(table, ctx));
    ctx.shutdown();
}

