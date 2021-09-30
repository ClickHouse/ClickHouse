#include <gtest/gtest.h>

#include <utility>
#include <limits>
#include <set>

#include <Storages/MergeTree/IntersectionsIndexes.h>

#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

using namespace DB;

TEST(Coordinator, Simple)
{
    PartitionReadRequest request;
    request.partition_id = "a";
    request.part_name = "b";
    request.projection_name = "c";
    request.block_range = PartBlockRange{1, 2};
    request.mark_ranges = MarkRanges{{1, 2}, {3, 4}};

    ParallelReplicasReadingCoordinator coordinator;
    auto response = coordinator.handleRequest(request);

    ASSERT_FALSE(response.denied) << "Process request at first has to be accepted";

    ASSERT_EQ(response.mark_ranges.size(), request.mark_ranges.size());

    for (int i = 0; i < response.mark_ranges.size(); ++i) {
        EXPECT_EQ(response.mark_ranges[i], request.mark_ranges[i]);
    }

    response = coordinator.handleRequest(request);
    ASSERT_TRUE(response.denied) << "Process the same request second time";
}


TEST(Coordinator, TwoRequests)
{
    PartitionReadRequest first;
    first.partition_id = "a";
    first.part_name = "b";
    first.projection_name = "c";
    first.block_range = PartBlockRange{0, 0};
    first.mark_ranges = MarkRanges{{1, 1}, {2, 2}};

    auto second = first;
    second.mark_ranges = MarkRanges{{2, 2}, {3, 3}};

    ParallelReplicasReadingCoordinator coordinator;
    auto response = coordinator.handleRequest(first);

    ASSERT_FALSE(response.denied) << "First request must me accepted";

    ASSERT_EQ(response.mark_ranges.size(), first.mark_ranges.size());
    for (int i = 0; i < response.mark_ranges.size(); ++i) {
        EXPECT_EQ(response.mark_ranges[i], first.mark_ranges[i]);
    }

    response = coordinator.handleRequest(second);
    ASSERT_FALSE(response.denied) << "Process the same request second time";
    ASSERT_EQ(response.mark_ranges.size(), 1);
    ASSERT_EQ(response.mark_ranges.front(), (MarkRange{3, 3}));
}



TEST(Coordinator, Boundaries)
{
    {
        PartRangesIntersectionsIndex boundaries;

        boundaries.addPart(PartToRead{{1, 1}, "Test"});
        boundaries.addPart(PartToRead{{2, 2}, "Test"});
        boundaries.addPart(PartToRead{{3, 3}, "Test"});
        boundaries.addPart(PartToRead{{4, 4}, "Test"});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 4}), 4);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 5}), 4);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({0, 5}), 4);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 1}), 1);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 2}), 2);

        boundaries.addPart(PartToRead{{5, 5}, "Test"});
        boundaries.addPart(PartToRead{{0, 0}, "Test"});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({0, 5}), 6);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 1}), 1);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 2}), 2);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({0, 3}), 4);
    }

    {
        PartRangesIntersectionsIndex boundaries;
        boundaries.addPart(PartToRead{{1, 3}, "Test"});
        boundaries.addPart(PartToRead{{3, 5}, "Test"});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({2, 4}), 2);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({0, 6}), 2);
    }

    {
        PartRangesIntersectionsIndex boundaries;
        boundaries.addPart(PartToRead{{1, 3}, "Test"});
        boundaries.addPart(PartToRead{{4, 6}, "Test"});
        boundaries.addPart(PartToRead{{7, 9}, "Test"});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({2, 8}), 3);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({4, 6}), 1);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({3, 7}), 3);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({5, 7}), 2);
    }

    {
        PartRangesIntersectionsIndex boundaries;

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 1}), 0);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 3}), 0);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({0, 100500}), 0);
    }
}


TEST(Coordinator, MarkBoundaries)
{
    {
        MarkRangesIntersectionsIndex boundaries;

        boundaries.addRange({1, 2});
        boundaries.addRange({2, 3});
        boundaries.addRange({3, 4});
        boundaries.addRange({4, 5});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 4}), 3);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 5}), 4);
    }

    {
        MarkRangesIntersectionsIndex boundaries;
        boundaries.addRange({1, 3});
        boundaries.addRange({3, 5});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({3, 5}), 1);

        boundaries.addRange({5, 7});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({3, 5}), 1);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({3, 7}), 2);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({4, 6}), 2);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 7}), 3);
    }

    {
        MarkRangesIntersectionsIndex boundaries;
        boundaries.addRange({1, 3});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({3, 4}), 0);
    }

    {
        MarkRangesIntersectionsIndex boundaries;
        boundaries.addRange({1, 2});
        boundaries.addRange({3, 4});
        boundaries.addRange({5, 6});

        ASSERT_EQ(boundaries.numberOfIntersectionsWith({2, 3}), 0);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({4, 5}), 0);
        ASSERT_EQ(boundaries.numberOfIntersectionsWith({1, 6}), 3);
    }
}

TEST(Coordinator, Bug)
{
    std::vector<std::string> data =
    {
        R"({"begin_part":1,"close_marks":[63656],"end_part":96,"open_marks":[63416],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[25153,25160,25166,25183,25212,25232,25249,25265,25276,25282,25294,25335,25372,25397,27569],"end_part":96,"open_marks":[25147,25155,25162,25167,25197,25221,25238,25256,25269,25278,25283,25313,25351,25383,27485],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[64531,64539,64551,64558,64565,64580,64588,64637,64657,64671,64677,64691,64703,64716,65413],"end_part":96,"open_marks":[64505,64532,64540,64552,64561,64567,64583,64610,64638,64663,64673,64683,64696,64706,65328],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[61348],"end_part":96,"open_marks":[61108],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[68255,68440,68879],"end_part":96,"open_marks":[68251,68344,68739],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[69976,69993,70015,70020,70057,70065,70080,70084,70096,70102,70148,70223,70390],"end_part":96,"open_marks":[69937,69982,69999,70017,70029,70059,70067,70081,70087,70097,70123,70184,70347],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[57776],"end_part":96,"open_marks":[57536],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[59065,59572],"end_part":96,"open_marks":[59049,59348],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[52905],"end_part":96,"open_marks":[52665],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[48522,48563,48597,48789,48807],"end_part":96,"open_marks":[48427,48541,48580,48689,48801],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[55555,55562,55578,55584,55591,55611,55637,55642,55646,55658,55672,55682,55690,55700,55721,55728,55744,55757,55773,55793,55801,55815,55877],"end_part":96,"open_marks":[55550,55557,55564,55579,55585,55598,55612,55638,55644,55647,55663,55676,55685,55691,55710,55722,55734,55748,55758,55774,55795,55802,55845],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[51932],"end_part":96,"open_marks":[51692],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[29515],"end_part":96,"open_marks":[29275],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[54581],"end_part":96,"open_marks":[54341],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[46521,46983,47002],"end_part":96,"open_marks":[46515,46754,46997],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[41677,41679,41695,41725,41742,41774,41795,41812,41820,41851,41859,41865,41890,41914,41922,41939,41945,41949,41965,41970,41977,41987,41993,42009],"end_part":96,"open_marks":[41676,41678,41683,41707,41732,41756,41784,41802,41815,41834,41852,41861,41866,41895,41915,41925,41940,41946,41950,41966,41971,41978,41988,41994],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[66925],"end_part":96,"open_marks":[66685],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[34911,34919,34929,34936,34941,34951,34957,34964,34976,35159],"end_part":96,"open_marks":[34740,34914,34923,34931,34937,34945,34953,34959,34969,35132],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[20149,20405,20424,20441],"end_part":96,"open_marks":[20061,20273,20413,20432],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[45432],"end_part":96,"open_marks":[45192],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[31212],"end_part":96,"open_marks":[30972],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[38175,38191,38196,38200,38212,38253,38274,38320,38357,38378,38402,38408,38412,38437,38451,38467,38487,38499],"end_part":96,"open_marks":[38156,38182,38192,38197,38201,38232,38262,38296,38337,38365,38379,38403,38409,38420,38440,38452,38468,38488],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[11172,11184,11204,11213,11220,11225,11229,11236,11241,11245,11265,11275,11284,11289,11297,11321,11337,11353,11356,11372,11377,11381,11393,11585],"end_part":96,"open_marks":[11168,11177,11190,11207,11215,11221,11226,11231,11238,11242,11246,11269,11279,11286,11291,11298,11328,11345,11354,11360,11373,11378,11382,11510],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[15102],"end_part":96,"open_marks":[14862],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[36864,36870,36876],"end_part":96,"open_marks":[36634,36865,36871],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[43196,43208,43214,43225,43279,43286,43295,43301,43304,43311,43316,43322,43332,43338,43344,43357,43368,43380,43386,43399,43408,43427,43435,43447,43459,43463,43472,43478,43485],"end_part":96,"open_marks":[43187,43197,43210,43217,43228,43281,43287,43297,43302,43307,43313,43317,43326,43334,43340,43345,43359,43370,43381,43389,43401,43409,43429,43437,43452,43460,43466,43474,43480],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[33324],"end_part":96,"open_marks":[33084],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[1895,2235,2448],"end_part":96,"open_marks":[1891,2065,2382],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[39955,39975,40425],"end_part":96,"open_marks":[39865,39964,40286],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[21649,21654,21672,21676,21681,21688,21700,21710,21720,21735,21742,21747,21756,21765,21773,21779,21803,21809,21815,21819,21824,21831,21838,21845,21858,21867,21875,21895,21903,21909,21913,21916,21919,21925,21931,21936,21939],"end_part":96,"open_marks":[21637,21650,21655,21673,21677,21683,21689,21705,21714,21721,21738,21744,21748,21757,21768,21775,21780,21805,21811,21816,21820,21827,21833,21839,21846,21860,21869,21877,21898,21905,21910,21914,21917,21920,21927,21932,21937],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[28542],"end_part":96,"open_marks":[28302],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[17400],"end_part":96,"open_marks":[17160],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[12832,12847,12904,12909,12915,12923,12929,12936,12939,12966,13001,13034,13052,13075,13081,13096,13182,13187,13244],"end_part":96,"open_marks":[12815,12839,12875,12906,12910,12916,12925,12932,12937,12940,12983,13016,13042,13060,13077,13082,13138,13184,13235],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[8060],"end_part":96,"open_marks":[7820],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[1,335,384,656],"end_part":96,"open_marks":[0,165,359,612],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[7087],"end_part":96,"open_marks":[6847],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[10337],"end_part":96,"open_marks":[10097],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[3659,3665,3683,3785,3880,3885,3969,4011,4050,4069,4111],"end_part":96,"open_marks":[3653,3661,3668,3733,3831,3881,3926,3989,4030,4059,4096],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[23364,23418,23455,23479,23507,23519,23530,23558,23882],"end_part":96,"open_marks":[23344,23386,23435,23466,23488,23512,23524,23541,23776],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[18283,18296,18302,18310,18315,18344,18368,18382,18397,18401,18416,18465,18470,18519,18630,18666,18685],"end_part":96,"open_marks":[18272,18284,18298,18304,18311,18328,18355,18374,18386,18398,18402,18439,18466,18494,18574,18645,18679],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[63896],"end_part":96,"open_marks":[63656],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[69039,69090,69096,69100,69164,69177,69179],"end_part":96,"open_marks":[68879,69063,69092,69097,69131,69165,69178],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[166,360,431],"end_part":96,"open_marks":[0,334,383],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[5162],"end_part":96,"open_marks":[4922],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[1873,1892,2066,2292],"end_part":96,"open_marks":[1869,1886,1894,2234],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[613,893,948],"end_part":96,"open_marks":[431,841,942],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[3634,3642,3652,3662,3669,3734,3832,3882,3927,3990,4031,4060,4069],"end_part":96,"open_marks":[3629,3637,3645,3656,3663,3672,3784,3879,3884,3968,4010,4049,4068],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[11134,11160,11178,11191,11208,11216,11222,11227,11232,11239,11243,11247,11253,11270,11280,11287,11292,11305,11329,11346,11361,11374,11383,11449],"end_part":96,"open_marks":[11130,11147,11171,11183,11196,11211,11219,11224,11228,11235,11240,11244,11248,11254,11273,11283,11288,11294,11306,11336,11352,11364,11376,11384],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[6134],"end_part":96,"open_marks":[5894],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[9040],"end_part":96,"open_marks":[8800],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[23338,23387,23436,23467,23489,23513,23525,23542,23664],"end_part":96,"open_marks":[23318,23363,23409,23454,23478,23498,23518,23529,23554],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[20052,20274,20414,20433,20447],"end_part":96,"open_marks":[19969,20148,20399,20423,20441],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[30795,31213],"end_part":96,"open_marks":[30561,31207],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[16218],"end_part":96,"open_marks":[15978],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[26469],"end_part":96,"open_marks":[26229],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[34240,34915,34924,34932,34938,34946,34954,34960,34970,34988],"end_part":96,"open_marks":[34056,34909,34918,34928,34935,34939,34950,34956,34961,34975],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[18288,18299,18305,18329,18356,18375,18387,18399,18440,18467,18495,18575,18646,18680,18695],"end_part":96,"open_marks":[18280,18289,18301,18307,18342,18367,18381,18393,18400,18464,18469,18518,18629,18662,18694],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[14729],"end_part":96,"open_marks":[14489],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[12840,12876,12907,12911,12926,12933,12941,12956,12984,13017,13043,13061,13078,13083,13139,13185,13210],"end_part":96,"open_marks":[12839,12846,12902,12908,12913,12928,12935,12942,12957,12998,13033,13051,13068,13079,13084,13181,13186],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[21646,21651,21656,21659,21674,21684,21690,21693,21706,21715,21722,21726,21729,21739,21745,21758,21761,21769,21776,21806,21812,21817,21828,21834,21840,21852,21861,21870,21873,21878,21899,21906,21911,21928,21933,21938,21941],"end_part":96,"open_marks":[21641,21647,21653,21657,21660,21675,21687,21691,21694,21709,21719,21723,21727,21730,21741,21746,21759,21762,21772,21778,21808,21814,21818,21830,21837,21842,21853,21862,21871,21874,21879,21902,21908,21912,21930,21935,21940],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[36392],"end_part":96,"open_marks":[36152],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[46484,46724],"end_part":96,"open_marks":[46448,46520],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[41641,41647,41672,41684,41708,41733,41757,41785,41803,41816,41835,41862,41867,41881,41883,41896,41916,41919,41926,41941,41947,41967,41972,41975],"end_part":96,"open_marks":[41640,41642,41648,41675,41686,41724,41740,41773,41794,41809,41819,41848,41863,41869,41882,41884,41900,41917,41920,41928,41943,41948,41968,41973],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[57972,58200],"end_part":96,"open_marks":[57776,58156],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[43124,43178,43182,43186,43211,43218,43229,43282,43288,43298,43308,43314,43327,43335,43341,43346,43382,43390,43394,43397,43402,43429],"end_part":96,"open_marks":[43104,43174,43180,43183,43187,43212,43221,43232,43283,43289,43300,43310,43315,43331,43337,43343,43347,43383,43392,43395,43398,43405],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[44681],"end_part":96,"open_marks":[44441],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[59635,59982,59985,59996],"end_part":96,"open_marks":[59572,59808,59983,59995],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[27441],"end_part":96,"open_marks":[27201],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[65653],"end_part":96,"open_marks":[65413],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[50115],"end_part":96,"open_marks":[49875],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[15234,15481],"end_part":96,"open_marks":[15102,15373],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[55513,55532,55551,55558,55565,55570,55580,55599,55613,55630,55633,55639,55648,55664,55677,55686,55692,55711,55723,55726,55735,55749,55759,55775,55796,55803,55812],"end_part":96,"open_marks":[55496,55520,55537,55552,55561,55568,55571,55582,55607,55614,55631,55634,55640,55649,55670,55681,55689,55693,55720,55724,55727,55740,55752,55762,55777,55798,55804],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[54725,54734,54738,54753,54764,54777,54788,54797,54810,54839,54846,54853],"end_part":96,"open_marks":[54581,54728,54735,54739,54754,54769,54781,54791,54799,54817,54841,54849],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[57284],"end_part":96,"open_marks":[57044],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[36878,36882,36889,36894,36899,36905,36910,36927,36978,37020,37051,37056,37084,37181,37249,37320],"end_part":96,"open_marks":[36876,36879,36883,36890,36896,36900,36906,36913,36952,36996,37034,37053,37057,37131,37211,37306],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[58703,59277],"end_part":96,"open_marks":[58676,59064],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[27809],"end_part":96,"open_marks":[27569],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[32649],"end_part":96,"open_marks":[32409],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[70470,70603,70704,70707,70719,70754,70768],"end_part":96,"open_marks":[70390,70537,70648,70705,70711,70735,70759],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[25163,25198,25222,25239,25257,25270,25279,25314,25352,25384,25497],"end_part":96,"open_marks":[25162,25165,25211,25231,25245,25264,25275,25281,25334,25371,25396],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[40571,40846],"end_part":96,"open_marks":[40425,40752],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[61036],"end_part":96,"open_marks":[60796],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[64493,64501,64533,64541,64545,64553,64562,64568,64584,64611,64634,64639,64664,64674,64684,64697,64707,64770],"end_part":96,"open_marks":[64467,64496,64502,64534,64543,64546,64555,64564,64570,64587,64632,64636,64640,64670,64676,64690,64702,64710],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[39835,39965,40117],"end_part":96,"open_marks":[39749,39954,39974],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[53833],"end_part":96,"open_marks":[53593],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[62653],"end_part":96,"open_marks":[62413],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[2383,2531,2534,2542,2550,2563,2571,2640,2752,2804],"end_part":96,"open_marks":[2292,2529,2532,2535,2544,2554,2565,2576,2701,2802],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[53145],"end_part":96,"open_marks":[52905],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[68148,68345,68570],"end_part":96,"open_marks":[68130,68254,68439],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[55880,55938,55989,56032,56039,56043,56048,56064,56069,56079,56101,56160,56167,56174,56251,56340],"end_part":96,"open_marks":[55877,55908,55962,56007,56034,56040,56044,56049,56066,56072,56081,56130,56163,56170,56203,56328],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[4097,4155,4184,4199,4206,4212,4224,4230,4241,4275,4296,4303,4311,4319,4325,4329,4336,4374,4421,4426,4429],"end_part":96,"open_marks":[4069,4125,4179,4186,4201,4208,4214,4225,4231,4249,4278,4297,4306,4314,4322,4327,4330,4338,4404,4422,4427],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[61378,61389,61402,61419,61428,61445,61459,61486,61539,61544,61552,61617,61633,61643,61650,61656,61667,61685,61704],"end_part":96,"open_marks":[61348,61379,61390,61410,61422,61436,61451,61472,61512,61541,61547,61581,61622,61637,61645,61652,61657,61668,61686],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[11511,11642,11754,11910,11978],"end_part":96,"open_marks":[11449,11633,11650,11855,11968],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[52172],"end_part":96,"open_marks":[51932],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[18705,18721,18730,18741,18751,18759,18765,18780,18805,18819,18827,18845,18848,18858,18870,18878,18973,19060,19080],"end_part":96,"open_marks":[18695,18714,18725,18732,18744,18754,18761,18767,18793,18814,18822,18830,18846,18849,18859,18873,18880,19055,19062],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[45578,45613,45617,45654,45662,45699,45706,45712,45718,45749],"end_part":96,"open_marks":[45432,45593,45614,45626,45656,45678,45701,45707,45714,45747],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[21954,21972,21985,22006,22032,22045,22054,22063,22072,22138,22249,22317,22348,22361,22370,22377],"end_part":96,"open_marks":[21941,21964,21979,21988,22023,22038,22050,22058,22064,22074,22200,22292,22339,22354,22367,22372],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[29577,29595,29600,29604,29619,29632,29639,29647,29655,29659,29666,29677,29681,29688,29691,29817,29889],"end_part":96,"open_marks":[29515,29584,29597,29601,29605,29625,29634,29641,29648,29656,29662,29671,29678,29682,29689,29753,29855],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[51087],"end_part":96,"open_marks":[50847],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[35291,35523],"end_part":96,"open_marks":[35159,35415],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[66140,66553],"end_part":96,"open_marks":[66113,66340],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[42016,42019,42024,42043,42052,42063,42069,42075,42083,42087,42117,42147,42169,42181,42186,42194,42254,42263,42279,42368],"end_part":96,"open_marks":[42010,42017,42020,42025,42045,42056,42065,42070,42078,42084,42088,42119,42148,42170,42182,42189,42223,42257,42270,42333],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[23777,24005,24131],"end_part":96,"open_marks":[23664,23998,24011],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[31224,31233,31618,31755],"end_part":96,"open_marks":[31212,31227,31424,31727],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[69908,69983,70000,70018,70030,70046,70060,70068,70082,70088,70124,70185,70221,70257],"end_part":96,"open_marks":[69873,69964,69988,70005,70019,70040,70047,70063,70070,70083,70090,70147,70219,70222],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[33550,33561,34247],"end_part":96,"open_marks":[33324,33555,34239],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[38129,38152,38183,38193,38198,38202,38233,38263,38297,38338,38366,38380,38398,38404,38410,38421,38432,38435,38441,38448,38453,38489,38493,38498],"end_part":96,"open_marks":[38125,38133,38171,38190,38194,38199,38203,38252,38271,38319,38356,38375,38381,38400,38405,38411,38430,38433,38436,38444,38449,38455,38490,38494],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[8264,8301,9604],"end_part":96,"open_marks":[8060,8282,9587],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[41981,42011,42021,42026,42046,42057,42066,42079,42085,42120,42127,42142,42149,42158,42183,42190,42224,42258,42271,42285],"end_part":96,"open_marks":[41975,41982,42013,42022,42028,42049,42062,42068,42081,42086,42122,42128,42144,42150,42159,42184,42193,42253,42262,42278],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[47013,47036,47052,47069,47104,47109,47114,47121,47135,47143,47343,47358,47384],"end_part":96,"open_marks":[47002,47024,47043,47059,47072,47105,47111,47115,47122,47139,47239,47348,47362],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[48363,48542,48581,48660],"end_part":96,"open_marks":[48227,48521,48562,48596],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[10577],"end_part":96,"open_marks":[10337],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[987,1120,1223,1315,1408],"end_part":96,"open_marks":[948,1029,1208,1237,1391],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[7327],"end_part":96,"open_marks":[7087],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[46755,46998,47025,47044,47060,47073,47106,47112,47123,47140,47235],"end_part":96,"open_marks":[46724,46982,47011,47035,47051,47066,47075,47107,47113,47125,47142],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[17640],"end_part":96,"open_marks":[17400],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[13236,13308,13354,13382,13388,13392,13408,13415,13422,13437,13457,13471,13481,13500,13505,13539,13585,13618,13632],"end_part":96,"open_marks":[13210,13283,13330,13375,13384,13390,13393,13411,13418,13423,13445,13464,13474,13484,13502,13507,13564,13603,13630],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[20442,20454,20461,20466,20481,20487,20500,20510,20515,20536,20546,20554,20578,20595,20607,20613,20622,20644,20647,20652,20703,20721,20735,20758,20800],"end_part":96,"open_marks":[20441,20446,20455,20463,20467,20482,20488,20505,20511,20516,20540,20549,20555,20585,20598,20608,20617,20628,20645,20648,20676,20711,20727,20745,20781],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[25737],"end_part":96,"open_marks":[25497],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[43492,43500,43511,43516,43519,43523,43529,43548,43567,43575,43582,43593,43598,43608,43613,43627,43640,43674,43701,43709,43714,43744,43750,43755,43766,43781,43983],"end_part":96,"open_marks":[43485,43494,43502,43512,43517,43520,43524,43538,43556,43570,43578,43585,43594,43602,43610,43620,43632,43644,43687,43703,43710,43721,43745,43751,43758,43767,43953],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[43430,43438,43453,43467,43475,43481,43495,43503,43521,43539,43557,43571,43579,43586,43595,43603,43611,43621,43633,43645,43688,43704,43711,43722,43740,43746,43759,43767],"end_part":96,"open_marks":[43429,43432,43440,43458,43470,43476,43483,43497,43505,43522,43547,43566,43574,43581,43589,43596,43607,43612,43626,43639,43649,43700,43708,43713,43732,43741,43747,43761],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[38511,38745,38845,38887,39071],"end_part":96,"open_marks":[38499,38627,38795,38866,39032],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[42334,42391,42402,42414,42417,42433,42484,42539,42560,42568,42576,42587,42594,42609,42653,42661,42665],"end_part":96,"open_marks":[42285,42387,42393,42406,42415,42418,42437,42527,42545,42566,42569,42579,42588,42596,42615,42657,42664],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[28782],"end_part":96,"open_marks":[28542],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[47240,47349,47363,47371,47374,47421,47605],"end_part":96,"open_marks":[47235,47332,47354,47368,47372,47375,47447],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[11634,11651,11856,11969,12036],"end_part":96,"open_marks":[11585,11641,11753,11909,12018],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[43768,43954,44189],"end_part":96,"open_marks":[43767,43770,44134],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[67032,67041,67046,67057,67066,67079,67657],"end_part":96,"open_marks":[66925,67036,67042,67050,67061,67068,67556],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[20464,20468,20479,20483,20506,20512,20517,20522,20541,20550,20556,20586,20599,20605,20618,20629,20677,20712,20728,20746,20782],"end_part":96,"open_marks":[20453,20465,20469,20480,20484,20509,20514,20518,20523,20545,20553,20557,20594,20603,20606,20621,20635,20702,20720,20734,20755],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[48814,48818,48825,48876,48885,48890,48894,48898,48904,48914,48921,48934,48944,48967,48976,48983,48987,49012,49069,49134,49160,49174],"end_part":96,"open_marks":[48807,48815,48820,48848,48878,48887,48891,48895,48899,48908,48916,48922,48935,48945,48968,48977,48984,48991,49038,49100,49146,49169],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[20816,20857,20893,20930,20990,21037,21123,21200,21236],"end_part":96,"open_marks":[20805,20825,20886,20894,20962,21017,21057,21167,21229],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[21944,21965,21980,21989,22025,22039,22051,22059,22069,22075,22201,22293,22341,22355,22368,22374],"end_part":96,"open_marks":[21939,21953,21971,21984,22005,22031,22044,22053,22060,22071,22137,22248,22316,22347,22360,22369],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[13284,13331,13379,13385,13404,13412,13420,13427,13448,13465,13475,13496,13503,13512,13567,13604,13626],"end_part":96,"open_marks":[13244,13307,13353,13381,13387,13407,13414,13421,13436,13456,13470,13480,13499,13504,13538,13584,13617],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[842,943,989],"end_part":96,"open_marks":[656,892,986],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[22377,22387,22854],"end_part":96,"open_marks":[22374,22381,22623],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[13631,13657,13723,13727,13783,13810,13842,13853,13864,13869,13880,13887,13896,13913,13922,13928,13945,13952,13968,13985,13989,13999,14092],"end_part":96,"open_marks":[13626,13641,13689,13724,13754,13796,13822,13846,13857,13866,13871,13883,13890,13897,13914,13923,13930,13946,13959,13975,13986,13992,14088],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[1030,1209,1238,1392,1572],"end_part":96,"open_marks":[989,1119,1222,1314,1557],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[68740,69064,69093,69098,69132,69166],"end_part":96,"open_marks":[68570,69037,69089,69095,69099,69163],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[69179,69184,69197,69208,69228,69243,69254,69266,69269,69286,69290,69303,69320,69330,69334,69347,69351,69358,69363,69390,69402,69413,69424,69432,69452,69481,69501,69508],"end_part":96,"open_marks":[69167,69180,69185,69198,69216,69238,69247,69260,69267,69270,69287,69291,69306,69321,69331,69335,69348,69352,69359,69365,69398,69405,69419,69427,69434,69467,69493,69507],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[55846,55909,55963,56008,56035,56041,56050,56067,56073,56082,56131,56164,56171,56196],"end_part":96,"open_marks":[55812,55877,55937,55988,56026,56036,56042,56052,56068,56076,56084,56159,56166,56173],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[2538,2545,2559,2566,2577,2702,2803,2810,2818],"end_part":96,"open_marks":[2448,2541,2549,2562,2570,2639,2751,2806,2812],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[56204,56237,56329,56463,56551,56619,56661,56669],"end_part":96,"open_marks":[56196,56234,56238,56403,56521,56581,56657,56663],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[64136],"end_part":96,"open_marks":[63896],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[18696,18715,18726,18737,18746,18755,18762,18768,18795,18815,18823,18868,18874,18892,19056,19063,19140],"end_part":96,"open_marks":[18685,18704,18720,18729,18740,18750,18758,18764,18779,18804,18818,18826,18869,18877,18972,19059,19137],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[1721,1728,1734,1741,1754,1793,1796,1799,1804,1814,1824,1833,1840,1858,1888],"end_part":96,"open_marks":[1572,1723,1730,1735,1745,1773,1794,1797,1800,1808,1818,1825,1834,1848,1872],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[65010],"end_part":96,"open_marks":[64770],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[12098,12124,12146,12157,12162,12186,12207,12219,12229,12234,12242,12246,12293,12325,12361],"end_part":96,"open_marks":[12036,12109,12134,12150,12158,12172,12195,12212,12222,12230,12236,12243,12247,12307,12338],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[59349,59802],"end_part":96,"open_marks":[59277,59634],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[59809,59996,60014,60021,60023,60029,60034,60039,60044,60123,60264,60363,60402],"end_part":96,"open_marks":[59802,59977,60007,60016,60022,60024,60030,60035,60041,60045,60199,60324,60399],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[19212,19265,19341,19351,19362,19367,19371,19378,19416,19446,19454,19469,19478,19485,19488,19495,19499,19505],"end_part":96,"open_marks":[19140,19237,19302,19346,19355,19363,19368,19372,19396,19430,19449,19460,19472,19481,19486,19489,19496,19500],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[4129,4181,4196,4202,4209,4215,4218,4222,4228,4232,4264,4272,4279,4282,4285,4289,4298,4307,4316,4323,4333,4344,4424,4437,6354],"end_part":96,"open_marks":[4111,4154,4183,4198,4205,4211,4216,4219,4223,4229,4240,4265,4274,4280,4283,4286,4290,4302,4310,4318,4324,4335,4373,4425,6345],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[23999,24012,24381],"end_part":96,"open_marks":[23882,24004,24266],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[2820,2843,2853,2920,2926,2965,2999,3031,3056,3059,3077,3095,3099,3105,3110,3125,3131,3198,3324],"end_part":96,"open_marks":[2818,2831,2845,2884,2922,2944,2981,3014,3040,3057,3067,3085,3096,3101,3107,3111,3126,3162,3305],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[70348,70538,70649,70712,70732],"end_part":96,"open_marks":[70257,70469,70602,70694,70717],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[70736,70760,70785,70813,70834,70849,70857,70867,70878,70892,70910,70921,70945,70963,70971,70978,70983,70987,71001,71015,71023,71026,71032,71043,71052,71065,71074,71134,71188,71206,71244,71288,71292,71306,71362,71403,71433,71466,71497,71515],"end_part":96,"open_marks":[70732,70753,70767,70800,70824,70842,70850,70858,70874,70880,70901,70917,70924,70952,70967,70974,70980,70984,70988,71002,71022,71024,71027,71036,71044,71054,71068,71076,71175,71198,71212,71276,71289,71293,71307,71391,71412,71452,71479,71513],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[38628,38796,38867,38921],"end_part":96,"open_marks":[38498,38744,38844,38886],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[39033,39237,39258,39270,39275,39302,39309,39324],"end_part":96,"open_marks":[38921,39181,39238,39267,39271,39276,39304,39310],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[6594],"end_part":96,"open_marks":[6354],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[44441],"end_part":96,"open_marks":[44189],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[35133,35385],"end_part":96,"open_marks":[34988,35290],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[35416,35642,35752,35755,35763,35767,35793,35821,35843,35873,35890],"end_part":96,"open_marks":[35385,35539,35742,35753,35756,35764,35768,35807,35831,35851,35879],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[40287,40640],"end_part":96,"open_marks":[40117,40570],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[40753,41048],"end_part":96,"open_marks":[40640,40921],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[69192,69199,69217,69239,69248,69282,69298,69316,69349,69356,69361,69380,69399,69406,69420,69428,69436,69468,69494,69508,69517],"end_part":96,"open_marks":[69179,69193,69207,69227,69242,69253,69283,69302,69317,69350,69357,69362,69389,69401,69412,69423,69431,69451,69480,69500,69514],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[14856,15346],"end_part":96,"open_marks":[14729,15233],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[69539,69576,69588,69601,69629,69634,69639,69675,69687,69700,69709,69722,69743,69755,69790,69817,69849,69937],"end_part":96,"open_marks":[69517,69540,69579,69592,69614,69631,69635,69655,69680,69692,69704,69711,69732,69747,69772,69803,69826,69907],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[5402],"end_part":96,"open_marks":[5162],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[6346,8283,8308],"end_part":96,"open_marks":[6134,8263,8300],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[8548],"end_part":96,"open_marks":[8308],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[36399,36866,36872,36875,36880,36891,36897,36901,36914,36918,36953,36997,37035,37054,37058,37132,37212,37253],"end_part":96,"open_marks":[36392,36862,36869,36873,36876,36881,36893,36898,36902,36916,36919,36977,37019,37050,37055,37059,37180,37246],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[58340,58351,58796],"end_part":96,"open_marks":[58200,58345,58702],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[9280],"end_part":96,"open_marks":[9040],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[60011,60017,60036,60042,60050,60202,60325,60400,60431],"end_part":96,"open_marks":[59996,60013,60018,60038,60043,60122,60263,60362,60419],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[16458],"end_part":96,"open_marks":[16218],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[24267,24627],"end_part":96,"open_marks":[24131,24523],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[2807,2813,2832,2846,2851,2885,2923,2945,2982,3015,3041,3053,3068,3086,3102,3108,3112,3116,3119,3127,3163,3212],"end_part":96,"open_marks":[2804,2809,2816,2842,2848,2852,2918,2925,2964,2998,3029,3050,3054,3076,3093,3104,3109,3113,3117,3120,3129,3196],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[26709],"end_part":96,"open_marks":[26469],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[31228,31425,31649],"end_part":96,"open_marks":[31213,31232,31617],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[3306,3417,3426,3431,3436,3442,3446,3457,3466,3474,3479,3484,3491,3500,3534,3569,3578,3587,3595,3601,3606,3629],"end_part":96,"open_marks":[3212,3410,3418,3428,3432,3438,3443,3447,3462,3469,3476,3481,3485,3492,3506,3553,3573,3582,3590,3596,3602,3607],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[31728,31899,31968,31974,31983,32035,32107],"end_part":96,"open_marks":[31649,31837,31960,31971,31975,31984,32078],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[56695,56717,56748,56793,56827,56844,56854,56870,56895,56898,56901,56922,56938,56957,56967,56982,56997,57008,57023,57044],"end_part":96,"open_marks":[56671,56711,56721,56770,56816,56837,56847,56857,56871,56896,56899,56903,56923,56941,56960,56969,56992,56999,57016,57029],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[37307,37402,37497,37566,37573,37597,37605,37608,37616,37630,37643,37659,37663,37671],"end_part":96,"open_marks":[37253,37368,37434,37562,37567,37574,37603,37606,37609,37621,37631,37644,37661,37664],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[44852,45594,45615,45627,45657,45675],"end_part":96,"open_marks":[44681,45577,45610,45616,45637,45659],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[24524,24786],"end_part":96,"open_marks":[24381,24689],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[60450,60471,60483,60494,60500,60514,60538,60546,60570,60591,60608,60614,60617,60621,60626,60635,60643,60650,60794,61108],"end_part":96,"open_marks":[60431,60456,60476,60488,60495,60505,60520,60539,60547,60579,60599,60610,60615,60618,60623,60627,60636,60644,60721,61091],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[47814,48195,48227],"end_part":96,"open_marks":[47605,48183,48196],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[15515,16907],"end_part":96,"open_marks":[15481,16701],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[48690,48802,48816,48821,48849,48879,48888,48892,48900,48909,48917,48923,48936,48992,49003,49039,49081],"end_part":96,"open_marks":[48660,48786,48813,48817,48824,48872,48882,48889,48893,48901,48913,48920,48924,48938,48995,49004,49066],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[49101,49147,49170,49193,49212,49239,49278,49304,49316,49320,49348,49356,49362,49367,49379,49391,49396,49402,49413,49453],"end_part":96,"open_marks":[49081,49133,49159,49179,49204,49216,49259,49295,49312,49317,49321,49351,49359,49364,49368,49383,49392,49397,49403,49414],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[41065,41216,41223,41235,41350,41459,41481,41561,41640],"end_part":96,"open_marks":[41048,41212,41218,41228,41236,41445,41471,41489,41631],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[40922,41213,41219,41230,41233,41235],"end_part":96,"open_marks":[40846,41064,41215,41222,41231,41234],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[41257,41446,41472,41490,41637,41645,41650,41668,41676],"end_part":96,"open_marks":[41235,41349,41458,41480,41560,41638,41646,41651,41671],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[42388,42397,42412,42427,42438,42532,42553,42571,42585,42591,42603,42622,42648,42658,42665,42668,42679,42686,42721],"end_part":96,"open_marks":[42368,42390,42401,42413,42432,42483,42538,42559,42575,42586,42593,42608,42623,42652,42660,42666,42671,42680,42706],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[62893],"end_part":96,"open_marks":[62653],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[65893],"end_part":96,"open_marks":[65653],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[4431,4670],"end_part":96,"open_marks":[4429,4432],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[12019,12110,12135,12151,12159,12173,12196,12213,12223,12231,12237,12244,12248,12269,12308,12339,12359,12367,12375],"end_part":96,"open_marks":[11978,12069,12122,12145,12156,12160,12184,12206,12218,12228,12233,12238,12245,12249,12270,12324,12357,12360,12374],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[34487],"end_part":96,"open_marks":[34247],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[12381,12384,12392,12396,12415,12474,12580,12646,12697,12760,12806,12839],"end_part":96,"open_marks":[12375,12382,12385,12393,12397,12416,12532,12642,12647,12740,12778,12831],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[54854,54865,54874,54890,54893,54911,54947,54958,54968,54982,54991,54998,55001,55015,55022,55061,55066,55077,55082,55095,55104,55124,55150,55175,55179],"end_part":96,"open_marks":[54853,54855,54866,54876,54891,54894,54928,54951,54963,54973,54983,54993,54999,55007,55017,55038,55062,55071,55078,55087,55098,55106,55125,55152,55176],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[42672,42684,42707,42754,42796,42825,42843,42934,43035,43060,43104],"end_part":96,"open_marks":[42665,42675,42685,42727,42779,42811,42837,42845,43019,43048,43071],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[9844],"end_part":96,"open_marks":[9604],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[10817],"end_part":96,"open_marks":[10577],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[39328,39340,39351,39378,39389,39398,39402,39555,39749],"end_part":96,"open_marks":[39324,39329,39344,39352,39382,39394,39400,39403,39710],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[15374,15726],"end_part":96,"open_marks":[15346,15514],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[15978],"end_part":96,"open_marks":[15726],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[7567],"end_part":96,"open_marks":[7327],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[22860,23104,23119,23130,23151,23245,23311,23344],"end_part":96,"open_marks":[22854,22977,23111,23123,23136,23197,23276,23337],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[64160,64251,64268,64279,64339,64355,64379,64384,64388,64416,64438,64455,64462,64473,64490,64499,64505],"end_part":96,"open_marks":[64136,64204,64259,64273,64306,64340,64366,64380,64385,64396,64417,64439,64458,64463,64474,64492,64500],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[53314,54088],"end_part":96,"open_marks":[53145,54017],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[32878,33556,33564],"end_part":96,"open_marks":[32649,33549,33560],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[42730,42780,42812,42841,42848,43020,43049,43072,43175,43187],"end_part":96,"open_marks":[42721,42753,42795,42824,42842,42933,43034,43059,43123,43177],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[54018,54729,54770,54782,54788],"end_part":96,"open_marks":[53833,54720,54731,54776,54787],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[52412],"end_part":96,"open_marks":[52172],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[57497,57998],"end_part":96,"open_marks":[57284,57971],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[17880],"end_part":96,"open_marks":[17640],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[54792,54800,54808,54818,54842,54850,54856,54869,54877,54887,54929,54952,54964,54974,54994,55008,55018,55039,55072,55079,55088,55099,55107,55129],"end_part":96,"open_marks":[54788,54796,54802,54809,54825,54843,54853,54857,54870,54879,54888,54946,54957,54967,54980,54997,55014,55021,55056,55076,55080,55094,55103,55110],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[44135,44939],"end_part":96,"open_marks":[43983,44851],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[55153,55177,55194,55236,55269,55277,55305,55331,55336,55346,55366,55382,55386,55397,55413,55420,55424,55427,55431,55445,55465,55470,55476,55491,55496],"end_part":96,"open_marks":[55130,55156,55178,55195,55264,55272,55281,55326,55332,55338,55349,55379,55384,55387,55404,55414,55422,55425,55428,55433,55455,55466,55472,55477,55494],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[45192],"end_part":96,"open_marks":[44939],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[27486,29585,29598,29602,29626,29635,29642,29649,29657,29663,29672,29679,29754,29856,29898],"end_part":96,"open_marks":[27441,29576,29591,29599,29603,29631,29638,29644,29650,29658,29665,29676,29680,29816,29894],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[28049],"end_part":96,"open_marks":[27809],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[50355],"end_part":96,"open_marks":[50115],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[29929,29967,29974,29978,29983,29996,30017,30021,30024,30033,30038,30045,30066,30080,30088,30185,30281,30287,30291],"end_part":96,"open_marks":[29898,29961,29970,29975,29980,29984,29997,30018,30022,30025,30035,30039,30046,30073,30084,30090,30275,30283,30288],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[60420,60457,60467,60477,60489,60496,60506,60521,60548,60580,60600,60611,60624,60628,60637,60641,60645,60722,60796],"end_part":96,"open_marks":[60402,60438,60465,60468,60482,60493,60497,60510,60526,60549,60590,60607,60613,60625,60629,60638,60642,60646,60793],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[56405,56522,56582,56658,56666,56680,56712,56723],"end_part":96,"open_marks":[56340,56462,56550,56618,56660,56668,56694,56716],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[61092,61383,61411,61423,61437,61452,61473,61513,61542,61548,61582,61623,61631,61638,61646,61653,61658,61668],"end_part":96,"open_marks":[61036,61377,61384,61418,61426,61444,61458,61485,61538,61543,61551,61610,61628,61632,61642,61647,61655,61659],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[56724,56771,56817,56840,56851,56893,56907,56918,56934,56943,56953,56964,56971,56994,57000,57017,57030,57536],"end_part":96,"open_marks":[56723,56747,56792,56826,56843,56853,56894,56908,56919,56937,56944,56956,56966,56981,56996,57007,57022,57496],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[61669,61687,61691,61792,61993],"end_part":96,"open_marks":[61668,61670,61688,61692,61874],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[61709,61875,62198],"end_part":96,"open_marks":[61704,61791,62047],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[59049],"end_part":96,"open_marks":[58796],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[29022],"end_part":96,"open_marks":[28782],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[45780,45785,45788,45795,45809,45852,45869,46001,46010,46021,46026,46101,46148,46169],"end_part":96,"open_marks":[45749,45781,45786,45790,45796,45831,45860,45934,46002,46011,46022,46062,46124,46166],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[67897],"end_part":96,"open_marks":[67657],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[46185,46219,46224,46276,46292,46298,46310,46322,46325,46337,46350,46360,46377,46389,46397,46425,46435,46438,46441,46447,46515],"end_part":96,"open_marks":[46169,46200,46220,46225,46277,46293,46299,46314,46323,46327,46338,46351,46367,46382,46392,46403,46429,46436,46439,46442,46483],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[70769,70801,70825,70846,70859,70875,70882,70902,70918,70932,70936,70955,70958,70968,70975,70981,70985,70992,70999,71007,71028,71041,71046,71050,71060,71071,71089,71176,71199,71213,71286,71304,71332,71392,71414,71453,71480,71515],"end_part":96,"open_marks":[70768,70784,70812,70833,70848,70866,70877,70891,70909,70920,70933,70944,70956,70962,70970,70977,70982,70986,70993,71000,71014,71031,71042,71048,71051,71064,71073,71133,71187,71205,71243,71287,71305,71361,71402,71432,71465,71496],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[51327],"end_part":96,"open_marks":[51087],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[20806,20826,20895,20963,21018,21068,21079,21168,21230,21260],"end_part":96,"open_marks":[20800,20815,20856,20929,20989,21036,21069,21122,21199,21258],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[69515,69550,69580,69593,69615,69632,69636,69656,69681,69693,69705,69712,69733,69748,69773,69804,69827,69873],"end_part":96,"open_marks":[69508,69521,69551,69584,69596,69628,69633,69638,69674,69685,69699,69708,69714,69742,69753,69789,69816,69835],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[3414,3423,3429,3433,3444,3451,3463,3471,3477,3482,3488,3493,3514,3563,3574,3583,3593,3598,3609,3612,3615,3621,3629,3638,3646,3653],"end_part":96,"open_marks":[3324,3416,3425,3430,3435,3445,3456,3465,3473,3478,3483,3490,3499,3533,3568,3577,3586,3594,3600,3610,3613,3616,3622,3633,3641,3651],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[1558,1719,1724,1731,1736,1739,1746,1774,1809,1819,1826,1829,1835,1838,1849,1869],"end_part":96,"open_marks":[1408,1716,1720,1727,1733,1737,1740,1751,1792,1813,1823,1827,1830,1836,1839,1857],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[14178,14187,14330,14412,14479,14862],"end_part":96,"open_marks":[14092,14181,14259,14370,14438,14855],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[22382,22621],"end_part":96,"open_marks":[22377,22386],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[6847],"end_part":96,"open_marks":[6594],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[4922],"end_part":96,"open_marks":[4670],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[21290,21303,21351,21385,21408,21418,21434,21447,21459,21474,21489,21511,21518,21523,21531,21538,21545,21554,21566,21575,21583,21591,21614,21629,21637],"end_part":96,"open_marks":[21260,21292,21327,21367,21392,21409,21425,21440,21449,21465,21480,21492,21513,21519,21524,21532,21541,21546,21559,21570,21578,21585,21601,21620,21634],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[25977],"end_part":96,"open_marks":[25737],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[29895,29963,29971,29976,29993,30009,30015,30019,30030,30036,30058,30074,30085,30092,30277,30284,30287],"end_part":96,"open_marks":[29889,29928,29966,29973,29977,29995,30010,30016,30020,30032,30037,30065,30079,30087,30184,30280,30286],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[22624,22978,23112,23124,23137,23198,23277,23318],"end_part":96,"open_marks":[22621,22859,23097,23118,23129,23139,23243,23310],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[30298,30307,30316,30321,30327,30334,30345,30363,30367,30379,30972],"end_part":96,"open_marks":[30287,30301,30310,30317,30323,30330,30336,30346,30364,30368,30794],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[65250],"end_part":96,"open_marks":[65010],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[47394,47448,48015],"end_part":96,"open_marks":[47384,47420,47813],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[24690,24960,25069,25076,25080,25086,25094,25101,25105,25110,25119,25124,25132,25156,25162],"end_part":96,"open_marks":[24627,24857,25059,25071,25077,25081,25087,25095,25103,25106,25111,25121,25125,25133,25159],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[19138,19238,19303,19347,19356,19364,19369,19375,19397,19431,19450,19461,19473,19482,19490,19497,19512],"end_part":96,"open_marks":[19080,19211,19264,19340,19350,19359,19366,19370,19376,19414,19445,19453,19466,19476,19484,19491,19498],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[31838,31966,31972,31976,31991,32079,32128],"end_part":96,"open_marks":[31755,31898,31967,31973,31977,32034,32106],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[19517,19522,19526,19529,19559,19606,19636,19658,19681,19819,19969],"end_part":96,"open_marks":[19512,19519,19523,19527,19530,19586,19624,19646,19667,19683,19953],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[32135,32173,32184,32191,32197,33084],"end_part":96,"open_marks":[32128,32150,32177,32186,32193,32877],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[7820],"end_part":96,"open_marks":[7567],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[37369,37435,37569,37589,37610,37641,37645,37648,37656,37671],"end_part":96,"open_marks":[37320,37401,37496,37570,37596,37615,37642,37646,37649,37658],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[37672,37685,37805,37887,37938,38016,38058,38071,38079,38124,38134,38156],"end_part":96,"open_marks":[37671,37675,37743,37845,37912,37974,38037,38064,38075,38097,38128,38151],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[49180,49205,49219,49260,49296,49314,49335,49344,49352,49360,49365,49375,49394,49405,49411,49416,49600],"end_part":96,"open_marks":[49174,49192,49211,49238,49277,49303,49315,49336,49347,49355,49361,49366,49378,49395,49406,49412,49525],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[21259,21293,21296,21328,21368,21393,21410,21426,21441,21450,21466,21481,21493,21514,21520,21525,21542,21547,21551,21560,21571,21579,21586,21602,21621,21635,21641],"end_part":96,"open_marks":[21236,21287,21294,21297,21350,21384,21401,21411,21433,21446,21452,21473,21488,21495,21517,21522,21526,21544,21549,21552,21565,21574,21582,21588,21613,21628,21639],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[48197,48200,48205,48427],"end_part":96,"open_marks":[48015,48198,48201,48362],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[26229],"end_part":96,"open_marks":[25977],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[49634,49763,51692],"end_part":96,"open_marks":[49600,49698,51538],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[52665],"end_part":96,"open_marks":[52412],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[62048,62413],"end_part":96,"open_marks":[61993,62216],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[35540,35750,35760,35765,35776,35808,35832,35867,35888,35894,35899,35905,35911,35919,35922],"end_part":96,"open_marks":[35523,35641,35751,35762,35766,35792,35820,35842,35872,35890,35895,35900,35907,35914,35921],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[5642],"end_part":96,"open_marks":[5402],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[39222,39249,39285,39299,39307,39324],"end_part":96,"open_marks":[39071,39223,39257,39286,39301,39308],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[10097],"end_part":96,"open_marks":[9844],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[39335,39349,39354,39363,39366,39373,39383,39395,39408,39711,39865],"end_part":96,"open_marks":[39324,39339,39350,39355,39364,39367,39377,39388,39397,39554,39834],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[54341],"end_part":96,"open_marks":[54088],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[13642,13690,13725,13755,13797,13823,13847,13858,13867,13872,13884,13891,13898,13911,13915,13920,13924,13931,13935,13960,13976,13987,13993,14009],"end_part":96,"open_marks":[13632,13652,13722,13726,13782,13809,13835,13851,13863,13868,13873,13886,13894,13899,13912,13916,13921,13926,13932,13936,13967,13984,13988,13996],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[24858,25067,25072,25078,25084,25099,25117,25122,25147],"end_part":96,"open_marks":[24786,24959,25068,25073,25079,25085,25100,25118,25123],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[26949],"end_part":96,"open_marks":[26709],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[35924,35940,36634],"end_part":96,"open_marks":[35922,35925,36398],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[5894],"end_part":96,"open_marks":[5642],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[12379,12389,12394,12398,12407,12410,12424,12533,12643,12648,12741,12779,12815],"end_part":96,"open_marks":[12366,12380,12391,12395,12399,12408,12411,12473,12579,12645,12696,12759,12805],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[35891,35896,35901,35908,35915,35922,36152],"end_part":96,"open_marks":[35890,35892,35898,35902,35910,35918,35923],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[19507,19514,19520,19524,19531,19588,19625,19647,19677,19684,19954,20061],"end_part":96,"open_marks":[19505,19508,19516,19521,19525,19558,19605,19635,19657,19680,19818,20051],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[45679,45702,45708,45715,45748,45782,45791,45797,45801,45832,45861,45935,46003,46023,46063,46100],"end_part":96,"open_marks":[45675,45696,45704,45709,45717,45776,45784,45794,45799,45802,45851,45867,45999,46004,46025,46098],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[55185,55191,55201,55208,55265,55273,55282,55333,55341,55353,55380,55388,55411,55415,55418,55429,55434,55467,55487,55505,55523,55526,55550],"end_part":96,"open_marks":[55179,55186,55193,55202,55235,55268,55276,55304,55335,55345,55365,55381,55396,55412,55416,55419,55430,55444,55469,55490,55512,55524,55531],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[8800],"end_part":96,"open_marks":[8548],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[65941,66341,66685],"end_part":96,"open_marks":[65893,66139,66682],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[46125,46167,46201,46221,46226,46239,46271,46294,46315,46328,46347,46352,46355,46357,46368,46383,46393,46404,46430,46448],"end_part":96,"open_marks":[46100,46147,46184,46216,46222,46227,46240,46272,46295,46320,46330,46348,46353,46356,46358,46376,46388,46396,46410,46434],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[34740],"end_part":96,"open_marks":[34487],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[66683,67037,67043,67051,67062,67069,67150],"end_part":96,"open_marks":[66553,67026,67040,67044,67056,67065,67071],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[17160],"end_part":96,"open_marks":[16907],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[63133],"end_part":96,"open_marks":[62893],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[62217,63416],"end_part":96,"open_marks":[62198,63182],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[32151,32178,32187,32194,32409],"end_part":96,"open_marks":[32134,32164,32183,32190,32196],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[10875,10880,10885,10895,10911,10921,10929,10938,10959,10966,10973,10984,10991,10997,11015,11028,11033,11041,11044,11070,11094,11108,11121,11149,11168],"end_part":96,"open_marks":[10817,10877,10882,10889,10902,10915,10924,10932,10939,10961,10968,10975,10985,10993,10998,11016,11029,11035,11042,11056,11083,11100,11112,11133,11159],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[14089,14182,14260,14371,14439,14489],"end_part":96,"open_marks":[14009,14177,14186,14329,14411,14466],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[28302],"end_part":96,"open_marks":[28049],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[9520],"end_part":96,"open_marks":[9280],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[17903,17913,17922,17927,17932,17995,18049,18074,18102,18111,18130,18139,18147,18153,18166,18180,18187,18192,18196,18199,18202,18214,18225,18230,18235,18241,18246,18249,18255,18267],"end_part":96,"open_marks":[17880,17906,17916,17923,17928,17961,18021,18060,18085,18103,18112,18134,18143,18148,18158,18171,18181,18188,18193,18197,18200,18207,18218,18227,18232,18237,18242,18247,18250,18260],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[37676,37744,37846,37913,37975,38038,38065,38076,38098,38125],"end_part":96,"open_marks":[37671,37678,37802,37886,37937,38009,38057,38070,38078,38116],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[29275],"end_part":96,"open_marks":[29022],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[49526,49699,49875],"end_part":96,"open_marks":[49453,49633,49762],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[50595],"end_part":96,"open_marks":[50355],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[67390],"end_part":96,"open_marks":[67150],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[33804],"end_part":96,"open_marks":[33564],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[16698],"end_part":96,"open_marks":[16458],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[9588,10878,10883,10890,10903,10916,10925,10933,10940,10962,10969,10976,10979,10994,10999,11017,11020,11030,11036,11057,11084,11101,11113,11130],"end_part":96,"open_marks":[9520,10874,10879,10884,10894,10910,10919,10928,10937,10942,10965,10972,10977,10980,10995,11001,11018,11021,11032,11038,11068,11093,11107,11117],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[50847],"end_part":96,"open_marks":[50595],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[51539,53341],"end_part":96,"open_marks":[51327,53313],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[53593],"end_part":96,"open_marks":[53341],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})",
        R"({"begin_part":1,"close_marks":[58157,58346,58424],"end_part":96,"open_marks":[57998,58339,58350],"part_name":"201307_1_96_4","partition_id":"201307","projeciton_name":""})"
    };

    std::vector<PartitionReadRequest> requests;
    for (const auto & line : data)
    {
        PartitionReadRequest x;
        x.deserializeFromJSON(line);
        requests.push_back(x);
    }

    ParallelReplicasReadingCoordinator coordinator;

    for (const auto & request : requests)
    {
        auto response = coordinator.handleRequest(request);
    }
}
