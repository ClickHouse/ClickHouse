#include <Databases/MySQL/MySQLBinlog.h>
#include <Databases/MySQL/MySQLBinlogEventsDispatcher.h>
#include <Databases/MySQL/MySQLBinlogClient.h>
#include <gtest/gtest.h>
#include <filesystem>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

using namespace DB;
using namespace DB::MySQLReplication;

#define TRY_LOOP_IMPL(expr, timeout) \
    const unsigned long _test_step = (timeout) < 350 ? (timeout) / 7 + 1 : 50; \
    for (int _i = 0; _i < (timeout) && !(expr); _i += _test_step) \
        std::this_thread::sleep_for(std::chrono::milliseconds(_test_step)); \

#define TRY_ASSERT_EQ(expr, expected, timeout) \
do { \
    TRY_LOOP_IMPL(((expr) == (expected)), timeout) \
    ASSERT_EQ((expr), expected); \
} while (false)

#define TRY_ASSERT_TRUE(expr, timeout) \
    TRY_ASSERT_EQ((expr), true, timeout)

static std::string getTestDataRoot()
{
    static auto root = []() -> std::string
    {
        std::filesystem::path testdata_path("src/Databases/MySQL/tests/data");
        auto basedir = std::filesystem::current_path();
        while (basedir != basedir.parent_path())
        {
            if (std::filesystem::exists(basedir / testdata_path))
            {
                testdata_path = basedir / testdata_path;
                break;
            }
            basedir = basedir.parent_path();
        }
        auto path = basedir / testdata_path;
        return std::filesystem::exists(path) ? path.string() : "";
    }();
    return root;
}

static String getTestDataPath(const String & testdata_file)
{
    return (std::filesystem::path(getTestDataRoot()) / testdata_file).string();
}

class MySQLBinlog : public ::testing::Test
{
protected:
    void SetUp() override
    {
        if (getTestDataRoot().empty())
            GTEST_SKIP() << "Skipping all tests since no test data files found";
    }

    UInt64 timeout = 25000;
};

TEST_F(MySQLBinlog, positionEndLogPosOverflow)
{
    Position position;
    EventHeader header;
    header.event_size = 8161;
    header.log_pos = 4294958114;
    BinlogParser::updatePosition(std::make_shared<DryRunEvent>(EventHeader(header)), position);
    ASSERT_EQ(position.binlog_pos, header.log_pos);
    ASSERT_TRUE(position.binlog_name.empty());
    ASSERT_TRUE(position.gtid_sets.toString().empty());
    ASSERT_EQ(position.timestamp, 0);

    header.log_pos = 4294966149;
    BinlogParser::updatePosition(std::make_shared<DryRunEvent>(EventHeader(header)), position);
    ASSERT_EQ(position.binlog_pos, header.log_pos);
    UInt64 prev = position.binlog_pos;

    header.log_pos = 7014;
    BinlogParser::updatePosition(std::make_shared<DryRunEvent>(EventHeader(header)), position);
    ASSERT_EQ(position.binlog_pos, prev + header.event_size);
    prev = position.binlog_pos;

    header.event_size = 8107;
    header.log_pos = 15121;
    BinlogParser::updatePosition(std::make_shared<DryRunEvent>(EventHeader(header)), position);
    ASSERT_EQ(position.binlog_pos, prev + header.event_size);
    prev = position.binlog_pos;

    header.event_size = 8131;
    header.log_pos = 23252;
    BinlogParser::updatePosition(std::make_shared<DryRunEvent>(EventHeader(header)), position);
    ASSERT_EQ(position.binlog_pos, prev + header.event_size);

    position.binlog_pos = 4724501662;
    prev = position.binlog_pos;

    header.event_size = 8125;
    header.log_pos = 429542491;
    BinlogParser::updatePosition(std::make_shared<DryRunEvent>(EventHeader(header)), position);
    ASSERT_EQ(position.binlog_pos, prev + header.event_size);

    position.binlog_pos = 5474055640;
    prev = position.binlog_pos;

    header.event_size = 31;
    header.log_pos = 1179088375;
    BinlogParser::updatePosition(std::make_shared<DryRunEvent>(EventHeader(header)), position);
    ASSERT_EQ(position.binlog_pos, prev + header.event_size);

    position = {};
    header.log_pos = 4294965445;
    BinlogParser::updatePosition(std::make_shared<DryRunEvent>(EventHeader(header)), position);
    ASSERT_EQ(position.binlog_pos, header.log_pos);
    prev = position.binlog_pos;

    header.event_size = 7927;
    header.log_pos = 6076;
    BinlogParser::updatePosition(std::make_shared<DryRunEvent>(EventHeader(header)), position);
    ASSERT_EQ(position.binlog_pos, prev + header.event_size);
}

TEST_F(MySQLBinlog, positionEquals)
{
    Position p1;
    Position p2;
    ASSERT_EQ(p1, p2);
    p1.binlog_pos = 1;
    ASSERT_NE(p1, p2);
    p2.binlog_pos = 1;
    ASSERT_EQ(p1, p2);
    p1.gtid_sets.parse("a9d88f83-c14e-11ec-bb36-244bfedf7766:87828");
    ASSERT_NE(p1, p2);
    p2.gtid_sets.parse("a9d88f83-c14e-11ec-bb36-244bfedf7766:87828");
    ASSERT_EQ(p1, p2);
    p1.binlog_name = "name";
    ASSERT_NE(p1, p2);
    p2.binlog_name = "name";
    ASSERT_EQ(p1, p2);
}

TEST_F(MySQLBinlog, positionMultimaster)
{
    Position p1;
    Position p2;
    p1.gtid_sets.parse("f189aee3-3cd2-11ed-a407-fa163ea7d4ed:1-3602,ff9de833-3cd2-11ed-87b7-fa163e99d975:1-172");
    p2.gtid_sets.parse("ff9de833-3cd2-11ed-87b7-fa163e99d975:1-172");
    ASSERT_TRUE(p1.gtid_sets.contains(p2.gtid_sets));
    ASSERT_FALSE(p2.gtid_sets.contains(p1.gtid_sets));
    ASSERT_FALSE(BinlogParser::isNew(p1, p2));

    p2.gtid_sets = {};
    p2.gtid_sets.parse("ff9de833-3cd2-11ed-87b7-fa163e99d975:1-10");
    ASSERT_FALSE(BinlogParser::isNew(p1, p2));

    p2.gtid_sets = {};
    p2.gtid_sets.parse("ff9de833-3cd2-11ed-87b7-fa163e99d975:172");
    ASSERT_FALSE(BinlogParser::isNew(p1, p2));

    p2.gtid_sets = {};
    p2.gtid_sets.parse("ff9de833-3cd2-11ed-87b7-fa163e99d975:171-172");
    ASSERT_FALSE(BinlogParser::isNew(p1, p2));

    p2.gtid_sets = {};
    p2.gtid_sets.parse("ff9de833-3cd2-11ed-87b7-fa163e99d975:171-173");
    ASSERT_TRUE(BinlogParser::isNew(p1, p2));

    p2.gtid_sets = {};
    p2.gtid_sets.parse("ff9de833-3cd2-11ed-87b7-fa163e99d975:173");
    ASSERT_TRUE(BinlogParser::isNew(p1, p2));

    p2.gtid_sets = {};
    p2.gtid_sets.parse("xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:173");
    ASSERT_FALSE(BinlogParser::isNew(p1, p2));

    p2.gtid_sets = {};
    p2.gtid_sets.parse("f189aee3-3cd2-11ed-a407-fa163ea7d4ed:1-3602,ff9de833-3cd2-11ed-87b7-fa163e99d975:1-172");
    ASSERT_FALSE(BinlogParser::isNew(p1, p2));

    p2.gtid_sets = {};
    p2.gtid_sets.parse("f189aee3-3cd2-11ed-a407-fa163ea7d4ed:1-3602,ff9de833-3cd2-11ed-87b7-fa163e99d975:1-173");
    ASSERT_TRUE(BinlogParser::isNew(p1, p2));
}

static void testFile1(IBinlog & binlog, UInt64 timeout, bool filtered = false)
{
    BinlogEventPtr event;
    int count = 0;

    if (!filtered)
    {
        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
        ++count;
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, FORMAT_DESCRIPTION_EVENT);
        ASSERT_EQ(event->header.timestamp, 1651442421);
        ASSERT_EQ(event->header.event_size, 122);
        ASSERT_EQ(event->header.log_pos, 126);

        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
        ++count;
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, PREVIOUS_GTIDS_EVENT);
        ASSERT_EQ(event->header.timestamp, 1651442421);
        ASSERT_EQ(event->header.event_size, 71);
        ASSERT_EQ(event->header.log_pos, 197);
    }

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651475081);
    ASSERT_EQ(event->header.event_size, 79);
    ASSERT_EQ(event->header.log_pos, 276);

    auto gtid_event = std::static_pointer_cast<GTIDEvent>(event);
    ASSERT_TRUE(gtid_event);
    ASSERT_EQ(gtid_event->commit_flag, 0);
    GTIDSets gtid_expected;
    gtid_expected.parse("a9d88f83-c14e-11ec-bb36-244bfedf7766:87828");
    GTIDSets gtid_actual;
    gtid_actual.update(gtid_event->gtid);
    ASSERT_EQ(gtid_actual.toString(), gtid_expected.toString());

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, QUERY_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651475081);
    ASSERT_EQ(event->header.event_size, 73);
    ASSERT_EQ(event->header.log_pos, 349);

    if (!filtered)
    {
        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
        ++count;
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, TABLE_MAP_EVENT);
        ASSERT_EQ(event->header.timestamp, 1651475081);
        ASSERT_EQ(event->header.event_size, 48);
        ASSERT_EQ(event->header.log_pos, 397);

        auto table_event = std::static_pointer_cast<TableMapEvent>(event);
        ASSERT_TRUE(table_event);
        ASSERT_EQ(table_event->table_id, 7566);
        ASSERT_EQ(table_event->flags, 1);
        ASSERT_EQ(table_event->schema_len, 2u);
        ASSERT_EQ(table_event->schema, "db");
        ASSERT_EQ(table_event->table_len, 1u);
        ASSERT_EQ(table_event->table, "a");
        ASSERT_EQ(table_event->column_count, 4);
        std::vector<UInt8> column_type = {3u, 3u, 3u, 3u};
        ASSERT_EQ(table_event->column_type, column_type);
        std::vector<UInt16> column_meta = {0, 0, 0, 0};
        ASSERT_EQ(table_event->column_meta, column_meta);
        std::vector<UInt32> column_charset = {};
        ASSERT_EQ(table_event->column_charset, column_charset);
        ASSERT_EQ(table_event->default_charset, 255u);
    }

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, WRITE_ROWS_EVENT_V2);
    ASSERT_EQ(event->header.timestamp, 1651475081);
    ASSERT_EQ(event->header.event_size, 52);
    ASSERT_EQ(event->header.log_pos, 449);

    ASSERT_EQ(event->type(), MYSQL_UNPARSED_ROWS_EVENT);
    event = std::static_pointer_cast<UnparsedRowsEvent>(event)->parse();

    ASSERT_TRUE(event);
    auto write_event = std::static_pointer_cast<WriteRowsEvent>(event);
    ASSERT_TRUE(write_event);
    ASSERT_EQ(write_event->number_columns, 4);
    ASSERT_EQ(write_event->schema, "db");
    ASSERT_EQ(write_event->table, "a");
    ASSERT_EQ(write_event->rows.size(), 1);
    ASSERT_EQ(write_event->rows[0].getType(), Field::Types::Tuple);
    auto row_data = write_event->rows[0].safeGet<const Tuple &>();
    ASSERT_EQ(row_data.size(), 4u);
    ASSERT_EQ(row_data[0].safeGet<UInt64>(), 1u);
    ASSERT_EQ(row_data[1].safeGet<UInt64>(), 1u);
    ASSERT_EQ(row_data[2].safeGet<UInt64>(), 1u);
    ASSERT_EQ(row_data[3].safeGet<UInt64>(), 1u);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, XID_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651475081);
    ASSERT_EQ(event->header.event_size, 31);
    ASSERT_EQ(event->header.log_pos, 480);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651475244);
    ASSERT_EQ(event->header.event_size, 79);
    ASSERT_EQ(event->header.log_pos, 559);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, QUERY_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651475244);
    ASSERT_EQ(event->header.event_size, 82);
    ASSERT_EQ(event->header.log_pos, 641);

    if (!filtered)
    {
        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
        ++count;
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, TABLE_MAP_EVENT);
        ASSERT_EQ(event->header.timestamp, 1651475244);
        ASSERT_EQ(event->header.event_size, 48);
        ASSERT_EQ(event->header.log_pos, 689);
    }

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.timestamp, 1651475244);
    ASSERT_EQ(event->header.event_size, 70);
    ASSERT_EQ(event->header.type, UPDATE_ROWS_EVENT_V2);
    ASSERT_EQ(event->header.log_pos, 759);

    ASSERT_EQ(event->type(), MYSQL_UNPARSED_ROWS_EVENT);
    event = std::static_pointer_cast<UnparsedRowsEvent>(event)->parse();

    ASSERT_TRUE(event);
    auto update_event = std::static_pointer_cast<UpdateRowsEvent>(event);
    ASSERT_TRUE(update_event);
    ASSERT_EQ(update_event->number_columns, 4);
    ASSERT_EQ(update_event->schema, "db");
    ASSERT_EQ(update_event->table, "a");
    ASSERT_EQ(update_event->rows.size(), 2);
    ASSERT_EQ(update_event->rows[0].getType(), Field::Types::Tuple);
    row_data = update_event->rows[0].safeGet<const Tuple &>();
    ASSERT_EQ(row_data.size(), 4u);
    ASSERT_EQ(row_data[0].safeGet<UInt64>(), 1u);
    ASSERT_EQ(row_data[1].safeGet<UInt64>(), 1u);
    ASSERT_EQ(row_data[2].safeGet<UInt64>(), 1u);
    ASSERT_EQ(row_data[3].safeGet<UInt64>(), 1u);
    row_data = update_event->rows[1].safeGet<const Tuple &>();
    ASSERT_EQ(row_data.size(), 4u);
    ASSERT_EQ(row_data[0].safeGet<UInt64>(), 1u);
    ASSERT_EQ(row_data[1].safeGet<UInt64>(), 2u);
    ASSERT_EQ(row_data[2].safeGet<UInt64>(), 1u);
    ASSERT_EQ(row_data[3].safeGet<UInt64>(), 1u);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, XID_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651475244);
    ASSERT_EQ(event->header.event_size, 31);
    ASSERT_EQ(event->header.log_pos, 790);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651482394);
    ASSERT_EQ(event->header.event_size, 79);
    ASSERT_EQ(event->header.log_pos, 869);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, QUERY_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651482394);
    ASSERT_EQ(event->header.event_size, 82);
    ASSERT_EQ(event->header.log_pos, 951);

    if (!filtered)
    {
        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
        ++count;
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, TABLE_MAP_EVENT);
        ASSERT_EQ(event->header.timestamp, 1651482394);
        ASSERT_EQ(event->header.event_size, 48);
        ASSERT_EQ(event->header.log_pos, 999);
    }

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, UPDATE_ROWS_EVENT_V2);
    ASSERT_EQ(event->header.timestamp, 1651482394);
    ASSERT_EQ(event->header.event_size, 70);
    ASSERT_EQ(event->header.log_pos, 1069);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, XID_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651482394);
    ASSERT_EQ(event->header.event_size, 31);
    ASSERT_EQ(event->header.log_pos, 1100);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651483072);
    ASSERT_EQ(event->header.event_size, 79);
    ASSERT_EQ(event->header.log_pos, 1179);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, QUERY_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651483072);
    ASSERT_EQ(event->header.event_size, 82);
    ASSERT_EQ(event->header.log_pos, 1261);

    if (!filtered)
    {
        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
        ++count;
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, TABLE_MAP_EVENT);
        ASSERT_EQ(event->header.timestamp, 1651483072);
        ASSERT_EQ(event->header.event_size, 48);
        ASSERT_EQ(event->header.log_pos, 1309);
    }

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, UPDATE_ROWS_EVENT_V2);
    ASSERT_EQ(event->header.timestamp, 1651483072);
    ASSERT_EQ(event->header.event_size, 70);
    ASSERT_EQ(event->header.log_pos, 1379);

    ASSERT_EQ(binlog.getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:87828-87830");

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, XID_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651483072);
    ASSERT_EQ(event->header.event_size, 31);
    ASSERT_EQ(event->header.log_pos, 1410);

    ASSERT_EQ(binlog.getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:87828-87831");

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651483336);
    ASSERT_EQ(event->header.event_size, 79);
    ASSERT_EQ(event->header.log_pos, 1489);
    gtid_event = std::static_pointer_cast<GTIDEvent>(event);
    ASSERT_TRUE(gtid_event);
    ASSERT_EQ(gtid_event->commit_flag, 0);
    gtid_expected = {};
    gtid_expected.parse("a9d88f83-c14e-11ec-bb36-244bfedf7766:87832");
    gtid_actual = {};
    gtid_actual.update(gtid_event->gtid);
    ASSERT_EQ(gtid_actual.toString(), gtid_expected.toString());

    ASSERT_EQ(binlog.getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:87828-87831");

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, QUERY_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651483336);
    ASSERT_EQ(event->header.event_size, 82);
    ASSERT_EQ(event->header.log_pos, 1571);

    if (!filtered)
    {
        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
        ++count;
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, TABLE_MAP_EVENT);
        ASSERT_EQ(event->header.timestamp, 1651483336);
        ASSERT_EQ(event->header.event_size, 48);
        ASSERT_EQ(event->header.log_pos, 1619);
    }

    int total_count = filtered ? 37 : 48;
    for (; count < total_count; ++count)
        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));

    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, ROTATE_EVENT);
    ASSERT_EQ(event->header.timestamp, 1651528821);
    ASSERT_EQ(event->header.event_size, 44);
    ASSERT_EQ(event->header.log_pos, 3091);
    ASSERT_EQ(count, total_count);
    ASSERT_FALSE(binlog.tryReadEvent(event, 10));

    auto position = binlog.getPosition();
    ASSERT_EQ(position.binlog_pos, 4);
    ASSERT_EQ(position.binlog_name, "binlog.001391");
    ASSERT_EQ(position.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:87828-87836");
}

TEST_F(MySQLBinlog, binlogFromFile1)
{
    BinlogFromFile binlog;
    binlog.open(getTestDataPath("binlog.001390"));
    testFile1(binlog, timeout);
}

TEST_F(MySQLBinlog, binlogFromFactory1)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.001390"));
    auto binlog = f->createBinlog("");

    testFile1(*binlog, timeout);
}

TEST_F(MySQLBinlog, binlogFromFactory1ExecutedGtidSet)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.001390"));
    BinlogEventPtr event;

    auto binlog = f->createBinlog("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87828");

    ASSERT_TRUE(binlog->tryReadEvent(event, timeout));
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);
    ASSERT_EQ(event->header.log_pos, 559);

    auto gtid_event = std::static_pointer_cast<GTIDEvent>(event);
    ASSERT_TRUE(gtid_event);
    GTIDSets gtid_expected;
    gtid_expected.parse("a9d88f83-c14e-11ec-bb36-244bfedf7766:87829");
    GTIDSets gtid_actual;
    gtid_actual.update(gtid_event->gtid);
    ASSERT_EQ(gtid_actual.toString(), gtid_expected.toString());

    for (int count = 8; count < 48; ++count)
        ASSERT_TRUE(binlog->tryReadEvent(event, timeout));

    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, ROTATE_EVENT);
    auto position = binlog->getPosition();
    ASSERT_EQ(position.binlog_pos, 4);
    ASSERT_EQ(position.binlog_name, "binlog.001391");
    ASSERT_EQ(position.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87836");
    ASSERT_FALSE(binlog->tryReadEvent(event, 10));

    binlog = f->createBinlog("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87829");

    ASSERT_TRUE(binlog->tryReadEvent(event, timeout));
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);
    ASSERT_EQ(event->header.log_pos, 869);

    gtid_event = std::static_pointer_cast<GTIDEvent>(event);
    ASSERT_TRUE(gtid_event);
    gtid_expected = {};
    gtid_expected.parse("a9d88f83-c14e-11ec-bb36-244bfedf7766:87830");
    gtid_actual = {};
    gtid_actual.update(gtid_event->gtid);
    ASSERT_EQ(gtid_actual.toString(), gtid_expected.toString());

    for (int count = 13; count < 48; ++count)
        ASSERT_TRUE(binlog->tryReadEvent(event, timeout));

    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, ROTATE_EVENT);
    position = binlog->getPosition();
    ASSERT_EQ(position.binlog_pos, 4);
    ASSERT_EQ(position.binlog_name, "binlog.001391");
    ASSERT_EQ(position.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87836");
    ASSERT_FALSE(binlog->tryReadEvent(event, 10));

    binlog = f->createBinlog("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87834");

    ASSERT_TRUE(binlog->tryReadEvent(event, timeout));
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);
    ASSERT_EQ(event->header.log_pos, 2443);

    gtid_event = std::static_pointer_cast<GTIDEvent>(event);
    ASSERT_TRUE(gtid_event);
    gtid_expected = {};
    gtid_expected.parse("a9d88f83-c14e-11ec-bb36-244bfedf7766:87835");
    gtid_actual = {};
    gtid_actual.update(gtid_event->gtid);
    ASSERT_EQ(gtid_actual.toString(), gtid_expected.toString());

    for (int count = 38; count < 48; ++count)
        ASSERT_TRUE(binlog->tryReadEvent(event, timeout));

    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, ROTATE_EVENT);
    position = binlog->getPosition();
    ASSERT_EQ(position.binlog_pos, 4);
    ASSERT_EQ(position.binlog_name, "binlog.001391");
    ASSERT_EQ(position.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87836");
    ASSERT_FALSE(binlog->tryReadEvent(event, 10));
}

TEST_F(MySQLBinlog, binlogFromDispatcher1)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.001390"));
    BinlogEventsDispatcher d;
    auto b = d.start(f->createBinlog(""));
    testFile1(*b, timeout, true);
    ASSERT_EQ(d.getPosition().gtid_sets.toString(), b->getPosition().gtid_sets.toString());
}

static void testFile2(IBinlog & binlog, UInt64 timeout, bool filtered = false)
{
    BinlogEventPtr event;
    int count = 0;

    if (!filtered)
    {
        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
        ++count;
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, FORMAT_DESCRIPTION_EVENT);

        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
        ++count;
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, PREVIOUS_GTIDS_EVENT);
    }

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, QUERY_EVENT);

    if (!filtered)
    {
        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
        ++count;
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, TABLE_MAP_EVENT);
    }

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, UPDATE_ROWS_EVENT_V2);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, XID_EVENT);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);
    ASSERT_EQ(event->header.log_pos, 608);

    auto gtid_event = std::static_pointer_cast<GTIDEvent>(event);
    ASSERT_TRUE(gtid_event);
    ASSERT_EQ(gtid_event->commit_flag, 0);
    GTIDSets gtid_expected;
    gtid_expected.parse("a9d88f83-c14e-11ec-bb36-244bfedf7766:1059");
    GTIDSets gtid_actual;
    gtid_actual.update(gtid_event->gtid);
    ASSERT_EQ(gtid_actual.toString(), gtid_expected.toString());

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, QUERY_EVENT);
    ASSERT_EQ(event->header.log_pos, 701);

    if (!filtered)
    {
        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
        ++count;
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, TABLE_MAP_EVENT);
        ASSERT_EQ(event->header.log_pos, 760);
    }

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, UPDATE_ROWS_EVENT_V2);
    ASSERT_EQ(event->header.log_pos, 830);

    ASSERT_EQ(event->type(), MYSQL_UNPARSED_ROWS_EVENT);
    event = std::static_pointer_cast<UnparsedRowsEvent>(event)->parse();

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, XID_EVENT);
    ASSERT_EQ(event->header.log_pos, 861);

    ASSERT_TRUE(binlog.tryReadEvent(event, timeout));
    ++count;
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);
    ASSERT_EQ(event->header.log_pos, 940);
    gtid_event = std::static_pointer_cast<GTIDEvent>(event);
    ASSERT_TRUE(gtid_event);
    ASSERT_EQ(gtid_event->commit_flag, 0);
    gtid_expected = {};
    gtid_expected.parse("a9d88f83-c14e-11ec-bb36-244bfedf7766:1060");
    gtid_actual = {};
    gtid_actual.update(gtid_event->gtid);
    ASSERT_EQ(gtid_actual.toString(), gtid_expected.toString());

    int total_count = filtered ? 13 : 18;
    for (; count < total_count; ++count)
        ASSERT_TRUE(binlog.tryReadEvent(event, timeout));

    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, ROTATE_EVENT);
    ASSERT_EQ(event->header.log_pos, 1237);
    ASSERT_EQ(count, total_count);
    ASSERT_FALSE(binlog.tryReadEvent(event, 10));

    auto position = binlog.getPosition();
    ASSERT_EQ(position.binlog_pos, 4);
    ASSERT_EQ(position.binlog_name, "binlog.000017");
    ASSERT_EQ(binlog.getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
}

TEST_F(MySQLBinlog, binlogFromFile2)
{
    BinlogFromFile binlog;
    binlog.open(getTestDataPath("binlog.000016"));
    testFile2(binlog, timeout);
}

TEST_F(MySQLBinlog, binlogFromDispatcher2)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    BinlogEventsDispatcher d;
    auto b = d.start(f->createBinlog(""));
    testFile2(*b, timeout, true);
    ASSERT_EQ(d.getPosition().gtid_sets.toString(), b->getPosition().gtid_sets.toString());
}

TEST_F(MySQLBinlog, binlogsFromOneFile)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto d2 = std::make_shared<BinlogEventsDispatcher>("d2");
    auto b1 = d1->start(f->createBinlog(""));
    auto b2 = d2->start(f->createBinlog(""));

    testFile2(*b1, timeout, true);
    testFile2(*b2, timeout, true);

    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), b2->getPosition().gtid_sets.toString());
    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(b1->getPosition().binlog_pos, b2->getPosition().binlog_pos);
    ASSERT_EQ(b1->getPosition().binlog_pos, 4);
}

TEST_F(MySQLBinlog, empty)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    ASSERT_TRUE(d1->getDispatcherMetadata().binlogs.empty());
}

TEST_F(MySQLBinlog, binlogsAfterStart)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");

    auto b1 = d1->start(f->createBinlog(""));
    auto b2 = d1->start(f->createBinlog(""));
    ASSERT_FALSE(b2);

    testFile2(*b1, timeout, true);
    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
}

TEST_F(MySQLBinlog, metadata)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    ASSERT_TRUE(d1->getDispatcherMetadata().binlogs.empty());
    ASSERT_EQ(d1->getDispatcherMetadata().name, "d1");
    ASSERT_TRUE(d1->getDispatcherMetadata().position.gtid_sets.sets.empty());

    auto b1 = d1->start(f->createBinlog(""));
    ASSERT_TRUE(b1);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs.size(), 1);
    ASSERT_FALSE(d1->start(f->createBinlog("")));

    TRY_ASSERT_TRUE(!d1->getDispatcherMetadata().position.gtid_sets.sets.empty(), timeout);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs.size(), 1);

    testFile2(*b1, timeout, true);

    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs.size(), 1);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].position_read.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].position_write.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].size, 0);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].bytes, 0);
}

TEST_F(MySQLBinlog, catchingUp)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto d2 = std::make_shared<BinlogEventsDispatcher>("d2");
    ASSERT_TRUE(d1->getDispatcherMetadata().binlogs.empty());
    ASSERT_TRUE(d2->getDispatcherMetadata().binlogs.empty());

    d2->syncTo(d1);

    auto b1 = d1->start(f->createBinlog(""));
    auto b2 = d2->start(f->createBinlog(""));
    ASSERT_TRUE(b1);
    ASSERT_TRUE(b2);
    TRY_ASSERT_EQ(d1->getDispatcherMetadata().binlogs.size(), 2, timeout);
    ASSERT_FALSE(d1->getDispatcherMetadata().position.gtid_sets.sets.empty());
    ASSERT_EQ(d2->getDispatcherMetadata().binlogs.size(), 0);
    ASSERT_FALSE(d2->getDispatcherMetadata().position.gtid_sets.sets.empty());
    ASSERT_FALSE(d2->start(f->createBinlog("")));

    testFile2(*b1, timeout, true);
    testFile2(*b2, timeout, true);

    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), b2->getPosition().gtid_sets.toString());
    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(b1->getPosition().binlog_pos, b2->getPosition().binlog_pos);
    ASSERT_EQ(b1->getPosition().binlog_pos, 4);
    ASSERT_EQ(d2->getDispatcherMetadata().binlogs.size(), 0);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs.size(), 2);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].position_read.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].position_write.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].position_read.binlog_pos, 4);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].position_write.binlog_pos, 4);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].size, 0);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].bytes, 0);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[1].position_read.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[1].position_write.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[1].position_read.binlog_pos, 4);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[1].position_write.binlog_pos, 4);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[1].size, 0);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[1].bytes, 0);
}

TEST_F(MySQLBinlog, catchingUpFastMaster)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto d2 = std::make_shared<BinlogEventsDispatcher>("d2");

    d2->syncTo(d1);

    auto b1 = d1->start(f->createBinlog(""));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto b2 = d2->start(f->createBinlog(""));

    testFile2(*b1, timeout, true);
    testFile2(*b2, timeout, true);

    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), b2->getPosition().gtid_sets.toString());
    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(b1->getPosition().binlog_pos, b2->getPosition().binlog_pos);
    ASSERT_EQ(b1->getPosition().binlog_pos, 4);
    ASSERT_EQ(d2->getDispatcherMetadata().binlogs.size(), 0);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs.size(), 2);
}

TEST_F(MySQLBinlog, catchingUpFastSlave)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto d2 = std::make_shared<BinlogEventsDispatcher>("d2");

    d2->syncTo(d1);

    auto b2 = d2->start(f->createBinlog(""));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto b1 = d1->start(f->createBinlog(""));

    TRY_ASSERT_EQ(d1->getDispatcherMetadata().binlogs.size(), 2, timeout);
    ASSERT_EQ(d2->getDispatcherMetadata().binlogs.size(), 0);
    ASSERT_FALSE(d1->getDispatcherMetadata().position.gtid_sets.sets.empty());
    ASSERT_FALSE(d2->getDispatcherMetadata().position.gtid_sets.sets.empty());

    testFile2(*b1, timeout, true);
    testFile2(*b2, timeout, true);

    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), b2->getPosition().gtid_sets.toString());
    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(b1->getPosition().binlog_pos, b2->getPosition().binlog_pos);
    ASSERT_EQ(b1->getPosition().binlog_pos, 4);
    ASSERT_EQ(d2->getDispatcherMetadata().binlogs.size(), 0);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs.size(), 2);
}

TEST_F(MySQLBinlog, catchingUpWithoutWaiting)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto d2 = std::make_shared<BinlogEventsDispatcher>("d2");

    d2->syncTo(d1);

    auto b1 = d1->start(f->createBinlog(""));
    auto b2 = d2->start(f->createBinlog(""));

    testFile2(*b1, timeout, true);
    testFile2(*b2, timeout, true);

    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), b2->getPosition().gtid_sets.toString());
    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(b1->getPosition().binlog_pos, b2->getPosition().binlog_pos);
    ASSERT_EQ(b1->getPosition().binlog_pos, 4);
    TRY_ASSERT_EQ(d2->getDispatcherMetadata().binlogs.size(), 0, timeout);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs.size(), 2);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].position_read.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].position_read.binlog_pos, 4);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[1].position_read.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[1].position_read.binlog_pos, 4);
}

TEST_F(MySQLBinlog, catchingUpManyToOne)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d0 = std::make_shared<BinlogEventsDispatcher>("d0");
    std::vector<BinlogEventsDispatcherPtr> ds;
    int n = 10;
    for (int i = 0; i < n; ++i)
    {
        auto d = std::make_shared<BinlogEventsDispatcher>("r" + std::to_string(i));
        d->syncTo(d0);
        ds.push_back(d);
    }

    for (int i = 0; i < n; ++i)
        ASSERT_TRUE(ds[i]->getDispatcherMetadata().binlogs.empty());

    auto b0 = d0->start(f->createBinlog(""), "b");
    ASSERT_EQ(d0->getDispatcherMetadata().binlogs.size(), 1);
    ASSERT_EQ(d0->getDispatcherMetadata().binlogs[0].position_read.binlog_pos, 0);
    std::vector<BinlogPtr> bs;
    bs.resize(n);
    for (int i = 0; i < n; ++i)
        bs[i] = ds[i]->start(f->createBinlog(""), "b" + std::to_string(i));

    TRY_ASSERT_EQ(d0->getDispatcherMetadata().binlogs.size(), n + 1, timeout);
    ASSERT_FALSE(d0->getDispatcherMetadata().position.gtid_sets.sets.empty());
    for (int i = 0; i < n; ++i)
    {
        ASSERT_EQ(ds[i]->getDispatcherMetadata().binlogs.size(), 0);
        ASSERT_FALSE(ds[i]->getDispatcherMetadata().position.gtid_sets.sets.empty());
    }

    testFile2(*b0, timeout, true);
    for (int i = 0; i < n; ++i)
        testFile2(*bs[i], timeout, true);

    ASSERT_EQ(b0->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    ASSERT_EQ(b0->getPosition().binlog_pos, 4);

    for (int i = 0; i < n; ++i)
    {
        ASSERT_EQ(bs[i]->getPosition().gtid_sets.toString(), b0->getPosition().gtid_sets.toString());
        ASSERT_EQ(bs[i]->getPosition().binlog_pos, b0->getPosition().binlog_pos);
    }

    for (int i = 0; i < n; ++i)
        ASSERT_EQ(ds[i]->getDispatcherMetadata().binlogs.size(), 0);

    ASSERT_EQ(d0->getDispatcherMetadata().binlogs.size(), n + 1);
    for (int i = 0; i < n + 1; ++i)
    {
        ASSERT_EQ(d0->getDispatcherMetadata().binlogs[i].position_read.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
        ASSERT_EQ(d0->getDispatcherMetadata().binlogs[i].position_write.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
        ASSERT_EQ(d0->getDispatcherMetadata().binlogs[i].position_read.binlog_pos, 4);
        ASSERT_EQ(d0->getDispatcherMetadata().binlogs[i].position_write.binlog_pos, 4);
        ASSERT_EQ(d0->getDispatcherMetadata().binlogs[i].size, 0);
        ASSERT_EQ(d0->getDispatcherMetadata().binlogs[i].bytes, 0);
    }
}

TEST_F(MySQLBinlog, catchingUpStopApplier)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto d2 = std::make_shared<BinlogEventsDispatcher>("d2");

    d2->syncTo(d1);

    auto b1 = d1->start(f->createBinlog(""));
    ASSERT_TRUE(b1);
    d1 = nullptr;

    auto b2 = d2->start(f->createBinlog(""));
    ASSERT_TRUE(b2);
    testFile2(*b2, timeout, true);
    ASSERT_EQ(b2->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
}

TEST_F(MySQLBinlog, catchingUpOneToAllPrevious)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    std::vector<BinlogEventsDispatcherPtr> ds;
    int n = 10;
    for (int i = 0; i < n; ++i)
    {
        auto d = std::make_shared<BinlogEventsDispatcher>("d" + std::to_string(i));
        for (int j = 0; j < i; ++j)
            d->syncTo(ds[j]);
        ds.push_back(d);
    }

    for (int i = 0; i < n; ++i)
        ASSERT_TRUE(ds[i]->getDispatcherMetadata().binlogs.empty());

    std::vector<BinlogPtr> bs;
    bs.resize(n);
    for (int i = 0; i < n; ++i)
        bs[i] = ds[i]->start(f->createBinlog(""), "b" + std::to_string(i));

    auto check_dispatchers = [&]
    {
        int not_empty_count = 0;
        int ii = 0;
        for (int i = 0; i < n; ++i)
        {
            if (!ds[i]->getDispatcherMetadata().binlogs.empty())
            {
                ++not_empty_count;
                ii = i;
            }
        }
        return not_empty_count == 1 && ds[ii]->getDispatcherMetadata().binlogs.size() == n;
    };

    for (int i = 0; i < n; ++i)
        testFile2(*bs[i], timeout, true);

    TRY_ASSERT_TRUE(check_dispatchers(), timeout);

    for (int i = 1; i < n; ++i)
    {
        ASSERT_EQ(bs[i]->getPosition().gtid_sets.toString(), bs[0]->getPosition().gtid_sets.toString());
        ASSERT_EQ(bs[i]->getPosition().binlog_pos, bs[0]->getPosition().binlog_pos);
    }

    int i = 0;
    for (int j = 0; j < n; ++j)
    {
        auto bs_ = ds[j]->getDispatcherMetadata().binlogs;
        for (; i < bs_.size(); ++i)
        {
            ASSERT_EQ(bs_[i].position_read.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
            ASSERT_EQ(bs_[i].position_write.gtid_sets.toString(), bs_[i].position_write.gtid_sets.toString());
            ASSERT_EQ(bs_[i].position_read.binlog_pos, 4);
            ASSERT_EQ(bs_[i].position_write.binlog_pos, 4);
            ASSERT_EQ(bs_[i].size, 0);
            ASSERT_EQ(bs_[i].bytes, 0);
        }
    }
    ASSERT_EQ(i, n);
}

TEST_F(MySQLBinlog, catchingUpMaxBytes)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto d2 = std::make_shared<BinlogEventsDispatcher>("d2");

    d2->syncTo(d1);

    auto b1 = d1->start(f->createBinlog(""), "big");
    auto b2 = d2->start(f->createBinlog(""), "small", {}, 1, 10000);

    testFile2(*b2, timeout, true);
    TRY_ASSERT_EQ(d1->getDispatcherMetadata().binlogs.size(), 2, timeout);
    ASSERT_EQ(d1->getDispatcherMetadata().position.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1058-1060");
    testFile2(*b1, timeout, true);
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].position_write.gtid_sets.toString(), d1->getDispatcherMetadata().position.gtid_sets.toString());
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].position_read.gtid_sets.toString(), d1->getDispatcherMetadata().position.gtid_sets.toString());
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[1].position_write.gtid_sets.toString(), d1->getDispatcherMetadata().position.gtid_sets.toString());
    ASSERT_EQ(d1->getDispatcherMetadata().binlogs[1].position_read.gtid_sets.toString(), d1->getDispatcherMetadata().position.gtid_sets.toString());
}

TEST_F(MySQLBinlog, filterEvents)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.001390"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto d2 = std::make_shared<BinlogEventsDispatcher>("d2");
    auto b1 = d1->start(f->createBinlog(""), "b1", {"db"});
    auto b2 = d2->start(f->createBinlog(""), "b2", {"unknown_database"});

    BinlogEventPtr event;
    for (int i = 0; i < 37; ++i)
    {
        ASSERT_TRUE(b1->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        switch (event->header.type)
        {
            case WRITE_ROWS_EVENT_V1:
            case WRITE_ROWS_EVENT_V2:
            case DELETE_ROWS_EVENT_V1:
            case DELETE_ROWS_EVENT_V2:
            case UPDATE_ROWS_EVENT_V1:
            case UPDATE_ROWS_EVENT_V2:
                if (event->type() == MYSQL_UNPARSED_ROWS_EVENT)
                {
                    ASSERT_EQ(std::static_pointer_cast<RowsEvent>(event)->schema, "db");
                }
                break;
            default:
                break;
        }
    }

    ASSERT_FALSE(b1->tryReadEvent(event, 0));
    ASSERT_FALSE(event);

    for (int i = 0; i < 37; ++i)
    {
        ASSERT_TRUE(b2->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        switch (event->header.type)
        {
            case ROTATE_EVENT:
            case XID_EVENT:
            case QUERY_EVENT:
            case GTID_EVENT:
                break;
            default:
                if (event->type() != MYSQL_UNHANDLED_EVENT)
                    FAIL() << "Unexpected event: " << magic_enum::enum_name(event->header.type);
                break;
        }
    }

    ASSERT_FALSE(b2->tryReadEvent(event, 0));
    ASSERT_FALSE(event);

    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:87828-87836");
    ASSERT_EQ(b1->getPosition().binlog_pos, 4);
    ASSERT_EQ(b2->getPosition().gtid_sets.toString(), b1->getPosition().gtid_sets.toString());
    ASSERT_EQ(b2->getPosition().binlog_pos, b1->getPosition().binlog_pos);
    ASSERT_FALSE(b2->tryReadEvent(event, 0));
}

TEST_F(MySQLBinlog, filterEventsMultipleDatabases)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.001390"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto d2 = std::make_shared<BinlogEventsDispatcher>("d2");
    auto d3 = std::make_shared<BinlogEventsDispatcher>("d3");
    auto d4 = std::make_shared<BinlogEventsDispatcher>("d4");
    auto d5 = std::make_shared<BinlogEventsDispatcher>("d5");
    auto all_dbs = d1->start(f->createBinlog(""), "all_dbs");
    auto db = d2->start(f->createBinlog(""), "db", {"db"});
    auto aborted = d3->start(f->createBinlog(""), "aborted_full_sync", {"aborted_full_sync"});
    auto db_and_aborted = d4->start(f->createBinlog(""), "db_and_aborted", {"db", "aborted_full_sync"});
    auto unknown = d5->start(f->createBinlog(""), "unknown", {"unknown1", "unknown2"});

    BinlogEventPtr event;
    for (int i = 0; i < 37; ++i)
    {
        ASSERT_TRUE(all_dbs->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        switch (event->header.type)
        {
            case WRITE_ROWS_EVENT_V1:
            case WRITE_ROWS_EVENT_V2:
            case DELETE_ROWS_EVENT_V1:
            case DELETE_ROWS_EVENT_V2:
            case UPDATE_ROWS_EVENT_V1:
            case UPDATE_ROWS_EVENT_V2:
                ASSERT_EQ(event->type(), MYSQL_UNPARSED_ROWS_EVENT);
                break;
            default:
                break;
        }
    }

    ASSERT_FALSE(all_dbs->tryReadEvent(event, 0));
    ASSERT_FALSE(event);

    for (int i = 0; i < 37; ++i)
    {
        ASSERT_TRUE(db->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        switch (event->header.type)
        {
            case WRITE_ROWS_EVENT_V1:
            case WRITE_ROWS_EVENT_V2:
            case DELETE_ROWS_EVENT_V1:
            case DELETE_ROWS_EVENT_V2:
            case UPDATE_ROWS_EVENT_V1:
            case UPDATE_ROWS_EVENT_V2:
                if (event->type() == MYSQL_UNPARSED_ROWS_EVENT)
                {
                    ASSERT_EQ(std::static_pointer_cast<RowsEvent>(event)->schema, "db");
                }
                break;
            default:
                break;
        }
    }

    ASSERT_FALSE(db->tryReadEvent(event, 0));
    ASSERT_FALSE(event);

    for (int i = 0; i < 37; ++i)
    {
        ASSERT_TRUE(aborted->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        switch (event->header.type)
        {
            case WRITE_ROWS_EVENT_V1:
            case WRITE_ROWS_EVENT_V2:
            case DELETE_ROWS_EVENT_V1:
            case DELETE_ROWS_EVENT_V2:
            case UPDATE_ROWS_EVENT_V1:
            case UPDATE_ROWS_EVENT_V2:
                if (event->type() == MYSQL_UNPARSED_ROWS_EVENT)
                {
                    ASSERT_EQ(std::static_pointer_cast<RowsEvent>(event)->schema, "aborted_full_sync");
                }
                break;
            default:
                break;
        }
    }

    ASSERT_FALSE(aborted->tryReadEvent(event, 0));
    ASSERT_FALSE(event);

    for (int i = 0; i < 37; ++i)
    {
        ASSERT_TRUE(db_and_aborted->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        switch (event->header.type)
        {
            case WRITE_ROWS_EVENT_V1:
            case WRITE_ROWS_EVENT_V2:
            case DELETE_ROWS_EVENT_V1:
            case DELETE_ROWS_EVENT_V2:
            case UPDATE_ROWS_EVENT_V1:
            case UPDATE_ROWS_EVENT_V2:
            {
                ASSERT_EQ(event->type(), MYSQL_UNPARSED_ROWS_EVENT);
                auto schema = std::static_pointer_cast<RowsEvent>(event)->schema;
                ASSERT_TRUE(schema == "db" || schema == "aborted_full_sync");
            } break;
            default:
                break;
        }
    }

    ASSERT_FALSE(db_and_aborted->tryReadEvent(event, 0));
    ASSERT_FALSE(event);

    for (int i = 0; i < 37; ++i)
    {
        ASSERT_TRUE(unknown->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        switch (event->header.type)
        {
            case ROTATE_EVENT:
            case XID_EVENT:
            case QUERY_EVENT:
            case GTID_EVENT:
                break;
            default:
                ASSERT_EQ(event->type(), MYSQL_UNHANDLED_EVENT);
                break;
        }
    }

    ASSERT_FALSE(unknown->tryReadEvent(event, 0));
    ASSERT_FALSE(event);
}

TEST_F(MySQLBinlog, dispatcherStop)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto b1 = d1->start(f->createBinlog(""));
    ASSERT_TRUE(b1);
    d1 = nullptr;
    BinlogEventPtr event;
    EXPECT_THROW(for (int i = 0; i < 18 + 1; ++i) b1->tryReadEvent(event, timeout), DB::Exception);
}

TEST_F(MySQLBinlog, executedGTIDSet)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto b1 = d1->start(f->createBinlog("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-1058"), "b1");

    BinlogEventPtr event;
    ASSERT_TRUE(b1->tryReadEvent(event, timeout));
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);
    ASSERT_EQ(event->header.log_pos, 608);

    ASSERT_TRUE(b1->tryReadEvent(event, timeout));
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, QUERY_EVENT);
    ASSERT_EQ(event->header.log_pos, 701);

    for (int i = 0; i < 7; ++i)
        ASSERT_TRUE(b1->tryReadEvent(event, timeout));

    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, ROTATE_EVENT);
    ASSERT_EQ(event->header.log_pos, 1237);
    ASSERT_EQ(d1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-1060");
    ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-1060");
    ASSERT_FALSE(b1->tryReadEvent(event, 0));
}

TEST_F(MySQLBinlog, client)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    auto e = std::make_shared<BinlogClient>(f);

    auto b1 = e->createBinlog("", "b1");
    auto b2 = e->createBinlog("", "b2");
    testFile2(*b1, timeout, true);
    testFile2(*b2, timeout, true);

    auto b3 = e->createBinlog("", "b3");

    testFile2(*b3, timeout, true);

    b1 = nullptr;
    b2 = nullptr;

    auto b4 = e->createBinlog("", "b4");
    testFile2(*b4, timeout, true);

    b3 = nullptr;
    b4 = e->createBinlog("", "b4 2");
    testFile2(*b4, timeout, true);

    b1 = e->createBinlog("", "b1 2");
    b2 = e->createBinlog("", "b2 2");
    testFile2(*b1, timeout, true);

    b3 = e->createBinlog("", "b3 2");
    testFile2(*b2, timeout, true);

    b4 = e->createBinlog("", "b4 3");
    testFile2(*b3, timeout, true);
    testFile2(*b4, timeout, true);

    b1 = nullptr;
    b2 = nullptr;
    b3 = nullptr;
    b4 = nullptr;
    b1 = e->createBinlog("", "b1 3");
    b2 = e->createBinlog("", "b2 3");
    b3 = e->createBinlog("", "b3 3");
    b4 = e->createBinlog("", "b4 4");
    testFile2(*b4, timeout, true);
    testFile2(*b3, timeout, true);
    testFile2(*b2, timeout, true);
    testFile2(*b1, timeout, true);

    f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.000016"));
    e = std::make_shared<BinlogClient>(f);

    b4 = e->createBinlog("", "b4 5");
    b3 = e->createBinlog("", "b3 4");
    testFile2(*b4, timeout, true);
    b2 = e->createBinlog("", "b2 4");
    b1 = e->createBinlog("", "b1 4");
    testFile2(*b3, timeout, true);
    testFile2(*b1, timeout, true);
    testFile2(*b2, timeout, true);

    b1 = e->createBinlog("", "b1 5");
    b2 = e->createBinlog("", "b2 5");
    testFile2(*b1, timeout, true);
    testFile2(*b2, timeout, true);
    b1 = e->createBinlog("", "b1 6");
    testFile2(*b1, timeout, true);
    b1 = e->createBinlog("", "b1 7");
    testFile2(*b1, timeout, true);

    b3 = nullptr;
    b4 = nullptr;
    b1 = e->createBinlog("", "b1 8");
    b4 = e->createBinlog("", "b4 6");
    b3 = e->createBinlog("", "b3 5");
    testFile2(*b4, timeout, true);
    testFile2(*b3, timeout, true);
    testFile2(*b1, timeout, true);

    b2 = nullptr;
    b3 = nullptr;
    b4 = nullptr;
    b1 = nullptr;
    b1 = e->createBinlog("", "b1 9");
    testFile2(*b1, timeout, true);
}

TEST_F(MySQLBinlog, createBinlog)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.001390"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto b1 = d1->start(f->createBinlog(""), "b1");
    ASSERT_TRUE(b1);
    ASSERT_FALSE(d1->start(f->createBinlog("")));
    testFile1(*b1, timeout, true);
    ASSERT_FALSE(d1->start(f->createBinlog("")));
    b1 = nullptr;
    ASSERT_FALSE(d1->start(f->createBinlog("")));
}

TEST_F(MySQLBinlog, createBinlogAttach1)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.001390"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto b1_ = d1->start(f->createBinlog("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87828"), "b1_");
    ASSERT_TRUE(b1_);
    auto b1 = d1->attach("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87831", "b1");
    if (b1)
    {
        BinlogEventPtr event;
        ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87831");
        ASSERT_TRUE(b1->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, GTID_EVENT);
        ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87831");

        ASSERT_TRUE(b1->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, QUERY_EVENT);
        ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87831");

        ASSERT_TRUE(b1->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, UPDATE_ROWS_EVENT_V2);
        ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87831");

        ASSERT_TRUE(b1->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, XID_EVENT);
        ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87832");
        for (int i = 0; i < 17; ++i)
            ASSERT_TRUE(b1->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, ROTATE_EVENT);
        ASSERT_FALSE(b1->tryReadEvent(event, 10));
        ASSERT_EQ(b1->getPosition().binlog_pos, 4);
        ASSERT_EQ(b1->getPosition().binlog_name, "binlog.001391");
        ASSERT_EQ(b1->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87836");
        for (int i = 0; i < 33; ++i)
            ASSERT_TRUE(b1_->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, ROTATE_EVENT);
        ASSERT_EQ(d1->getDispatcherMetadata().binlogs.size(), 2);
        ASSERT_EQ(d1->getDispatcherMetadata().binlogs[0].bytes, 0);
        ASSERT_EQ(d1->getDispatcherMetadata().binlogs[1].bytes, 0);
    }
}

TEST_F(MySQLBinlog, createBinlogAttach2)
{
    BinlogEventPtr event;
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.001390"));
    auto d1 = std::make_shared<BinlogEventsDispatcher>("d1");
    auto d2 = std::make_shared<BinlogEventsDispatcher>("d2");
    auto d3 = std::make_shared<BinlogEventsDispatcher>("d3");
    auto d4 = std::make_shared<BinlogEventsDispatcher>("d4");

    auto b1 = d1->start(f->createBinlog("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87828"), "b1");
    ASSERT_TRUE(b1);
    ASSERT_TRUE(b1->tryReadEvent(event, timeout));
    ASSERT_TRUE(event);
    ASSERT_EQ(event->header.type, GTID_EVENT);

    auto b2_ = d2->start(f->createBinlog("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87828"), "b2_");
    ASSERT_TRUE(b2_);
    auto b2 = d2->attach("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87831", "b2");

    auto b3_ = d3->start(f->createBinlog("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87828"), "b3_");
    ASSERT_TRUE(b3_);
    auto b3 = d3->attach("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87835", "b3");

    auto b4_ = d4->start(f->createBinlog("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87828"), "b4_");
    ASSERT_TRUE(b4_);
    auto b4 = d4->attach("a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87828", "b4");

    /// There is a race with dispatcher thread
    if (b2)
    {
        ASSERT_TRUE(b2->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, GTID_EVENT);

        ASSERT_TRUE(b2->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, QUERY_EVENT);

        ASSERT_TRUE(b2->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, UPDATE_ROWS_EVENT_V2);
        for (int i = 0; i < 18; ++i)
            ASSERT_TRUE(b2->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, ROTATE_EVENT);
        ASSERT_FALSE(b2->tryReadEvent(event, 10));
        ASSERT_EQ(b2->getPosition().binlog_pos, 4);
        ASSERT_EQ(b2->getPosition().binlog_name, "binlog.001391");
        ASSERT_EQ(b2->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87836");
        for (int i = 0; i < 33; ++i)
            ASSERT_TRUE(b2_->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, ROTATE_EVENT);
        ASSERT_EQ(d2->getDispatcherMetadata().binlogs.size(), 2);
        ASSERT_EQ(d2->getDispatcherMetadata().binlogs[0].bytes, 0);
        ASSERT_EQ(d2->getDispatcherMetadata().binlogs[1].bytes, 0);
    }

    if (b4)
    {
        ASSERT_TRUE(b4->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, GTID_EVENT);

        ASSERT_TRUE(b4->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, QUERY_EVENT);

        ASSERT_TRUE(b4->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, WRITE_ROWS_EVENT_V2);
        for (int i = 0; i < 10; ++i)
            ASSERT_TRUE(b4->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, ROTATE_EVENT);
        ASSERT_FALSE(b2->tryReadEvent(event, 10));
        ASSERT_EQ(b4->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87836");
        for (int i = 0; i < 33; ++i)
            ASSERT_TRUE(b4_->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, ROTATE_EVENT);
        ASSERT_EQ(d4->getDispatcherMetadata().binlogs.size(), 2);
        ASSERT_EQ(d4->getDispatcherMetadata().binlogs[0].bytes, 0);
        ASSERT_EQ(d4->getDispatcherMetadata().binlogs[1].bytes, 0);
    }

    if (b3)
    {
        ASSERT_TRUE(b3->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, GTID_EVENT);

        ASSERT_TRUE(b3->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, QUERY_EVENT);
        for (int i = 0; i < 3; ++i)
            ASSERT_TRUE(b3->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, ROTATE_EVENT);
        ASSERT_FALSE(b3->tryReadEvent(event, 10));
        ASSERT_EQ(b3->getPosition().gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:1-87836");
        for (int i = 0; i < 33; ++i)
            ASSERT_TRUE(b3_->tryReadEvent(event, timeout));
        ASSERT_TRUE(event);
        ASSERT_EQ(event->header.type, ROTATE_EVENT);
        ASSERT_EQ(d3->getDispatcherMetadata().binlogs.size(), 2);
        ASSERT_EQ(d3->getDispatcherMetadata().binlogs[0].bytes, 0);
        ASSERT_EQ(d3->getDispatcherMetadata().binlogs[1].bytes, 0);
    }
}

TEST_F(MySQLBinlog, factoryThreads)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.001390"));
    auto func1 = [&]
    {
        auto b1 = f->createBinlog("");
        auto b2 = f->createBinlog("");
        auto b3 = f->createBinlog("");
        testFile1(*b1, timeout);
        testFile1(*b2, timeout);
        b2 = f->createBinlog("");
        testFile1(*b2, timeout);
        b1 = f->createBinlog("");
        testFile1(*b1, timeout);
        b1 = nullptr;
        b2 = f->createBinlog("");
        testFile1(*b2, timeout);
        b1 = f->createBinlog("");
        testFile1(*b1, timeout);
        testFile1(*b3, timeout);
    };

    auto func2 = [&]
    {
        auto b1 = f->createBinlog("");
        auto b2 = f->createBinlog("");
        testFile1(*b2, timeout);
        testFile1(*b1, timeout);
        b1 = f->createBinlog("");
        testFile1(*b1, timeout);
        b2 = f->createBinlog("");
        testFile1(*b2, timeout);
        b1 = f->createBinlog("");
        b2 = f->createBinlog("");
        testFile1(*b1, timeout);
        b2 = nullptr;
        b1 = f->createBinlog("");
        testFile1(*b1, timeout);
        b1 = nullptr;
    };

    int n = 4;
    std::vector<std::thread> ts1, ts2;
    for (int i = 0; i < n; ++i)
    {
        ts1.emplace_back(std::thread(func1));
        ts2.emplace_back(std::thread(func2));
    }
    for (int i = 0; i < n; ++i)
    {
        ts1[i].join();
        ts2[i].join();
    }
}

TEST_F(MySQLBinlog, clientThreads)
{
    auto f = std::make_shared<BinlogFromFileFactory>(getTestDataPath("binlog.001390"));
    auto e = std::make_shared<BinlogClient>(f);
    auto func1 = [&]
    {
        auto b1 = e->createBinlog("");
        auto b2 = e->createBinlog("");
        testFile1(*b1, timeout, true);
        testFile1(*b2, timeout, true);
        b1 = nullptr;
        b2 = nullptr;
        b2 = e->createBinlog("");
        testFile1(*b2, timeout, true);
        b1 = e->createBinlog("");
        testFile1(*b1, timeout, true);
        b1 = nullptr;
        b2 = e->createBinlog("");
        testFile1(*b2, timeout, true);
        b2 = nullptr;
        b1 = e->createBinlog("");
        testFile1(*b1, timeout, true);
    };

    auto func2 = [&]
    {
        auto b1 = e->createBinlog("");
        testFile1(*b1, timeout, true);
        auto b2 = e->createBinlog("");
        testFile1(*b2, timeout, true);
        b2 = e->createBinlog("");
        b1 = e->createBinlog("");
        testFile1(*b1, timeout, true);
        testFile1(*b2, timeout, true);
        b1 = nullptr;
        b2 = nullptr;
        b1 = e->createBinlog("");
        testFile1(*b1, timeout, true);
        b2 = e->createBinlog("");
        testFile1(*b2, timeout, true);
    };

    int n = 4;
    std::vector<std::thread> ts1, ts2;
    for (int i = 0; i < n; ++i)
    {
        ts1.emplace_back(std::thread(func1));
        ts2.emplace_back(std::thread(func2));
    }
    for (int i = 0; i < n; ++i)
    {
        ts1[i].join();
        ts2[i].join();
    }

    // All dispatchers synced and finished
    // No dispatchers and no binlogs are alive here
    ASSERT_EQ(e->getMetadata().dispatchers.size(), 0);

    // Creates new dispatcher
    auto b1 = e->createBinlog("", "b1 1");
    testFile1(*b1, timeout, true);

    auto md = e->getMetadata().dispatchers;
    ASSERT_EQ(md.size(), 1);
    ASSERT_EQ(md[0].position.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:87828-87836");
    ASSERT_EQ(md[0].binlogs.size(), 1);
    ASSERT_EQ(md[0].binlogs[0].position_read.gtid_sets.toString(), "a9d88f83-c14e-11ec-bb36-244bfedf7766:87828-87836");
    ASSERT_EQ(md[0].binlogs[0].size, 0);
    ASSERT_EQ(md[0].binlogs[0].bytes, 0);

    // Creates new dispatcher
    auto b1_2 = e->createBinlog("", "b1 2");

    // Should sync to the first dispatcher
    TRY_ASSERT_EQ(e->getMetadata().dispatchers.size(), 1, timeout);
    // If there is no CPU available,
    // it possible to catch in the middle of the transform between dispatchers.
    // Checking again to make sure that catching up is finished.
    TRY_ASSERT_EQ(e->getMetadata().dispatchers.size(), 1, timeout);
    b1 = nullptr;
    md = e->getMetadata().dispatchers;
    ASSERT_EQ(md.size(), 1);
    ASSERT_EQ(md[0].binlogs.size(), 1);
    // Did not read any events yet
    ASSERT_EQ(md[0].binlogs[0].position_read.gtid_sets.toString(), "");
    ASSERT_EQ(md[0].binlogs[0].position_read.binlog_pos, 0);

    auto b2 = e->createBinlog("", "b2");

    BinlogEventPtr event;
    // Read only one event
    ASSERT_TRUE(b2->tryReadEvent(event, timeout));
    // Waits before all binlogs are moved to main dispatcher
    TRY_ASSERT_EQ(e->getMetadata().dispatchers[0].binlogs.size(), 2, timeout);

    // One dispatcher is alive
    md = e->getMetadata().dispatchers;
    ASSERT_EQ(md.size(), 1);
    ASSERT_EQ(md[0].binlogs.size(), 2);
    ASSERT_EQ(md[0].binlogs[0].position_read.gtid_sets.toString(), "");
    ASSERT_EQ(md[0].binlogs[1].position_read.gtid_sets.toString(), "");
    ASSERT_EQ(md[0].binlogs[0].position_read.binlog_pos, md[0].binlogs[0].name == "b2" ? 276 : 0); // Read one event
    ASSERT_EQ(md[0].binlogs[1].position_read.binlog_pos, md[0].binlogs[0].name == "b2" ? 0 : 276);
}
