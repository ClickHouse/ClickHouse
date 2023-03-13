//#include <Functions/keyvaluepair/src/KeyValuePairExtractorBuilder.h>
//#include <gtest/gtest.h>

//
//namespace DB
//{
//
//struct LazyKeyValuePairExtractorTestCase
//{
//    std::string input;
//    std::unordered_map<std::string, std::string> expected_output;
//    std::shared_ptr<KeyValuePairExtractor> extractor;
//};
//
//std::ostream & operator<<(std::ostream & ostr, const LazyKeyValuePairExtractorTestCase & test_case)
//{
//    return ostr << test_case.input;
//}
//
//struct KeyValuePairExtractorTest : public ::testing::TestWithParam<LazyKeyValuePairExtractorTestCase>
//{
//};
//
//TEST_P(KeyValuePairExtractorTest, KeyValuePairExtractorTests)
//{
//    const auto & [input, expected_output, extractor] = GetParam();
//
//    auto result = extractor->extract(input);
//
//    EXPECT_EQ(result, expected_output);
//}
//
//INSTANTIATE_TEST_SUITE_P(
//    ValuesCanBeEmptyString,
//    KeyValuePairExtractorTest,
//    ::testing::ValuesIn(std::initializer_list<LazyKeyValuePairExtractorTestCase>{
//        {
//            "age:",
//            {
//                {
//                    "age",
//                    ""
//                }
//            }, KeyValuePairExtractorBuilder().withEscapingProcessor<SimpleKeyValuePairEscapingProcessor>().build()
//        },
//        {
//            "name: neymar, favorite_movie:,favorite_song:",
//            {
//                {"name", "neymar"},
//                {"favorite_movie", ""},
//                {"favorite_song", ""},
//            },
//         KeyValuePairExtractorBuilder().withEscapingProcessor<SimpleKeyValuePairEscapingProcessor>().build()
//        }
//    }));
//
//INSTANTIATE_TEST_SUITE_P(
//    MixString,
//    KeyValuePairExtractorTest,
//    ::testing::ValuesIn(std::initializer_list<LazyKeyValuePairExtractorTestCase>{
//        {
//            R"(9 ads =nm, no\:me: neymar, age: 30, daojmskdpoa and a height: 1.75, school: lupe\ picasso, team: psg,)",
//            {
//                {
//                    R"(no:me)", "neymar"
//                },
//                {
//                    "age", "30"
//                },
//                {
//                    "height", "1.75"
//                },
//                {
//                    "school", "lupe picasso"
//                },
//                {
//                    "team", "psg"
//                }
//            },
//         KeyValuePairExtractorBuilder()
//                .withValueSpecialCharacterAllowlist({'.'})
//                .build()
//        },
//        {
//            "XNFHGSSF_RHRUZHVBS_KWBT: F,",
//            {
//                {
//                    "XNFHGSSF_RHRUZHVBS_KWBT",
//                    "F"
//                }
//            },
//         KeyValuePairExtractorBuilder().build()
//        },
//    }));
//
//INSTANTIATE_TEST_SUITE_P(
//    Escaping,
//    KeyValuePairExtractorTest,
//    ::testing::ValuesIn(std::initializer_list<LazyKeyValuePairExtractorTestCase>{
//        {
//            "na,me,: neymar, age:30",
//            {
//                {
//                    "age", "30"
//                }
//            },
//         KeyValuePairExtractorBuilder().build()
//        },
//        {
//            "na$me,: neymar, age:30",
//            {
//                {
//                    "age", "30"
//                }
//            },
//            KeyValuePairExtractorBuilder().build()
//        },
//        {
//            R"(name: neymar, favorite_quote: Premature\ optimization\ is\ the\ r\$\$t\ of\ all\ evil, age:30)",
//            {
//                {
//                    "name", "neymar"
//                },
//                {
//                    "favorite_quote", R"(Premature optimization is the r$$t of all evil)"
//                },
//                {
//                    "age", "30"
//                }
//            },
//         KeyValuePairExtractorBuilder()
//             .withEnclosingCharacter('"')
//             .build()
//        }}));
//
//INSTANTIATE_TEST_SUITE_P(
//    EnclosedElements,
//    KeyValuePairExtractorTest,
//    ::testing::ValuesIn(std::initializer_list<LazyKeyValuePairExtractorTestCase>{
//        {
//            R"("name": "Neymar", "age": 30, team: "psg", "favorite_movie": "", height: 1.75)",
//            {
//                {
//                    "name", "Neymar"
//                },
//                {
//                    "age", "30"
//                },
//                {
//                    "team", "psg"
//                },
//                {
//                    "favorite_movie", ""
//                },
//                {
//                    "height", "1.75"
//                }
//            },
//         KeyValuePairExtractorBuilder()
//                .withValueSpecialCharacterAllowlist({'.'})
//                .withEnclosingCharacter('"')
//                .build()
//        }
//    }));
//
//}
