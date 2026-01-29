#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsEmbeddedDictionaries.h>


namespace DB
{

REGISTER_FUNCTION(EmbeddedDictionaries)
{
    {
        FunctionDocumentation::Description description = R"(
Accepts a region ID from the geobase. If this region is a city or part of a city, it returns the region ID for the appropriate city.
        )";
        FunctionDocumentation::Syntax syntax = "regionToCity(id[, geobase])";
        FunctionDocumentation::Arguments arguments = {
            {"id", "Region ID from the geobase", {"UInt32"}},
            {"geobase", "Optional. The dictionary key.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the region ID for the appropriate city, if it exists, otherwise returns `0`", {"UInt32"}};
        FunctionDocumentation::Examples examples = {
            {
                "Usage example",
                "SELECT regionToName(number::UInt32, 'en'), regionToCity(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);",
                R"(
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCity(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                          │
│ World                                      │  0 │                                                          │
│ USA                                        │  0 │                                                          │
│ Colorado                                   │  0 │                                                          │
│ Boulder County                             │  0 │                                                          │
│ Boulder                                    │  5 │ Boulder                                                  │
│ China                                      │  0 │                                                          │
│ Sichuan                                    │  0 │                                                          │
│ Chengdu                                    │  8 │ Chengdu                                                  │
│ America                                    │  0 │                                                          │
│ North America                              │  0 │                                                          │
│ Eurasia                                    │  0 │                                                          │
│ Asia                                       │  0 │                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────┘
            )"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::EmbeddedDictionary;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionRegionToCity>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Converts a region to an area (type 5 in the geobase).
        )";
        FunctionDocumentation::Syntax syntax = "regionToArea(id[, geobase])";
        FunctionDocumentation::Arguments arguments = {
            {"id", "Region ID from the geobase", {"UInt32"}},
            {"geobase", "Optional. The dictionary key.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the region ID for the appropriate area, if it exists, otherwise returns `0`", {"UInt32"}};
        FunctionDocumentation::Examples examples = {
            {
                "Usage example",
                R"(
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
            )",
                R"(
┌─regionToName(regionToArea(toUInt32(number), \'ua\'))─┐
│                                                      │
│ Moscow and Moscow region                             │
│ St. Petersburg and Leningrad region                  │
│ Belgorod region                                      │
│ Ivanovsk region                                      │
│ Kaluga region                                        │
│ Kostroma region                                      │
│ Kursk region                                         │
│ Lipetsk region                                       │
│ Orlov region                                         │
│ Ryazan region                                        │
│ Smolensk region                                      │
│ Tambov region                                        │
│ Tver region                                          │
│ Tula region                                          │
└──────────────────────────────────────────────────────┘
            )"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::EmbeddedDictionary;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionRegionToArea>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Converts a region to a federal district (type 4 in the geobase).
        )";
        FunctionDocumentation::Syntax syntax = "regionToDistrict(id[, geobase])";
        FunctionDocumentation::Arguments arguments = {
            {"id", "Region ID from the geobase", {"UInt32"}},
            {"geobase", "Optional. The dictionary key.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the region ID for the appropriate district, if it exists, otherwise returns `0`", {"UInt32"}};
        FunctionDocumentation::Examples examples = {
            {
                "Usage example",
                "SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua')) FROM system.numbers LIMIT 15",
                R"(
┌─regionToName(regionToDistrict(toUInt32(number), 'ua'))─┐
│ Central federal district                               │
│ Northwest federal district                             │
│ South federal district                                 │
│ Far East federal district                              │
│ Siberia federal district                               │
│ Ural federal district                                  │
│ Volga federal district                                 │
│ Privolga federal district                              │
│ North Caucasus federal district                        │
│ Scotland                                               │
│ Faroe Islands                                          │
│ Flemish region                                         │
│ Brussels capital region                                │
│ Wallonia                                               │
└────────────────────────────────────────────────────────┘
            )"
        }
    };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::EmbeddedDictionary;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionRegionToDistrict>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Converts a region to a country (type 3 in the geobase).
        )";
        FunctionDocumentation::Syntax syntax = "regionToCountry(id[, geobase])";
        FunctionDocumentation::Arguments arguments = {
            {"id", "Region ID from the geobase", {"UInt32"}},
            {"geobase", "Optional. The dictionary key.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the region ID for the appropriate country, if it exists, otherwise returns `0`", {"UInt32"}};
        FunctionDocumentation::Examples examples = {
            {
                "Usage example",
                "SELECT regionToName(number::UInt32, 'en'), regionToCountry(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);",
                R"(
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToCountry(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                             │
│ World                                      │  0 │                                                             │
│ USA                                        │  2 │ USA                                                         │
│ Colorado                                   │  2 │ USA                                                         │
│ Boulder County                             │  2 │ USA                                                         │
│ Boulder                                    │  2 │ USA                                                         │
│ China                                      │  6 │ China                                                       │
│ Sichuan                                    │  6 │ China                                                       │
│ Chengdu                                    │  6 │ China                                                       │
│ America                                    │  0 │                                                             │
│ North America                              │  0 │                                                             │
│ Eurasia                                    │  0 │                                                             │
│ Asia                                       │  0 │                                                             │
└────────────────────────────────────────────┴────┴─────────────────────────────────────────────────────────────┘
            )"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::EmbeddedDictionary;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionRegionToCountry>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Converts a region to a continent (type 1 in the geobase).
        )";
        FunctionDocumentation::Syntax syntax = "regionToContinent(id[, geobase])";
        FunctionDocumentation::Arguments arguments = {
            {"id", "Region ID from the geobase", {"UInt32"}},
            {"geobase", "Optional. The dictionary key.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the region ID for the appropriate continent, if it exists, otherwise returns `0`", {"UInt32"}};
        FunctionDocumentation::Examples examples = {
            {
                "Usage example",
                "SELECT regionToName(number::UInt32, 'en'), regionToContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);",
                R"(
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                               │
│ World                                      │  0 │                                                               │
│ USA                                        │ 10 │ North America                                                 │
│ Colorado                                   │ 10 │ North America                                                 │
│ Boulder County                             │ 10 │ North America                                                 │
│ Boulder                                    │ 10 │ North America                                                 │
│ China                                      │ 12 │ Asia                                                          │
│ Sichuan                                    │ 12 │ Asia                                                          │
│ Chengdu                                    │ 12 │ Asia                                                          │
│ America                                    │  9 │ America                                                       │
│ North America                              │ 10 │ North America                                                 │
│ Eurasia                                    │ 11 │ Eurasia                                                       │
│ Asia                                       │ 12 │ Asia                                                          │
└────────────────────────────────────────────┴────┴───────────────────────────────────────────────────────────────┘
            )"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::EmbeddedDictionary;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionRegionToContinent>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Finds the highest continent in the hierarchy for the region.
        )";
        FunctionDocumentation::Syntax syntax = "regionToTopContinent(id[, geobase])";
        FunctionDocumentation::Arguments arguments = {
            {"id", "Region ID from the geobase", {"UInt32"}},
            {"geobase", "Optional. The dictionary key.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the identifier of the top level continent (the latter when you climb the hierarchy of regions), or `0` if there is none", {"UInt32"}};
        FunctionDocumentation::Examples examples = {
            {
                "Usage example",
                "SELECT regionToName(number::UInt32, 'en'), regionToTopContinent(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);",
                R"(
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─id─┬─regionToName(regionToTopContinent(CAST(number, 'UInt32')), 'en')─┐
│                                            │  0 │                                                                  │
│ World                                      │  0 │                                                                  │
│ USA                                        │  9 │ America                                                          │
│ Colorado                                   │  9 │ America                                                          │
│ Boulder County                             │  9 │ America                                                          │
│ Boulder                                    │  9 │ America                                                          │
│ China                                      │ 11 │ Eurasia                                                          │
│ Sichuan                                    │ 11 │ Eurasia                                                          │
│ Chengdu                                    │ 11 │ Eurasia                                                          │
│ America                                    │  9 │ America                                                          │
│ North America                              │  9 │ America                                                          │
│ Eurasia                                    │ 11 │ Eurasia                                                          │
│ Asia                                       │ 11 │ Eurasia                                                          │
└────────────────────────────────────────────┴────┴──────────────────────────────────────────────────────────────────┘
            )"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::EmbeddedDictionary;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionRegionToTopContinent>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Gets the population for a region. The population can be recorded in files with the geobase. See the section ["Dictionaries"](../dictionaries#embedded-dictionaries).
If the population is not recorded for the region, it returns `0`. In the geobase, the population might be recorded for child regions, but not for parent regions.
        )";
        FunctionDocumentation::Syntax syntax = "regionToPopulation(id[, geobase])";
        FunctionDocumentation::Arguments arguments = {
            {"id", "Region ID from the geobase", {"UInt32"}},
            {"geobase", "Optional. The dictionary key.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns the population for the region, or `0` if there is none", {"UInt32"}};
        FunctionDocumentation::Examples examples = {
            {
                "Usage example",
                "SELECT regionToName(number::UInt32, 'en'), regionToPopulation(number::UInt32) AS id, regionToName(id, 'en') FROM numbers(13);",
                R"(
┌─regionToName(CAST(number, 'UInt32'), 'en')─┬─population─┐
│                                            │          0 │
│ World                                      │ 4294967295 │
│ USA                                        │  330000000 │
│ Colorado                                   │    5700000 │
│ Boulder County                             │     330000 │
│ Boulder                                    │     100000 │
│ China                                      │ 1500000000 │
│ Sichuan                                    │   83000000 │
│ Chengdu                                    │   20000000 │
│ America                                    │ 1000000000 │
│ North America                              │  600000000 │
│ Eurasia                                    │ 4294967295 │
│ Asia                                       │ 4294967295 │
└────────────────────────────────────────────┴────────────┘
            )"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::EmbeddedDictionary;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionRegionToPopulation>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Checks whether a `lhs` region belongs to a `rhs` region. Returns a `UInt8` number equal to `1` if it belongs, or `0` if it does not belong.
        )";
        FunctionDocumentation::Syntax syntax = "regionIn(lhs, rhs[, geobase])";
        FunctionDocumentation::Arguments arguments = {
            {"lhs", "Lhs region ID from the geobase", {"UInt32"}},
            {"rhs", "Rhs region ID from the geobase", {"UInt32"}},
            {"geobase", "Optional. The dictionary key.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the region belongs, `0` otherwise", {"UInt8"}};
        FunctionDocumentation::Examples examples = {
            {
                "Usage example",
                "SELECT regionToName(n1.number::UInt32, 'en') || (regionIn(n1.number::UInt32, n2.number::UInt32) ? ' is in ' : ' is not in ') || regionToName(n2.number::UInt32, 'en') FROM numbers(1,2) AS n1 CROSS JOIN numbers(1,5) AS n2;",
                R"(
World is in World
World is not in USA
World is not in Colorado
World is not in Boulder County
World is not in Boulder
USA is in World
USA is in USA
USA is not in Colorado
USA is not in Boulder County
USA is not in Boulder
            )"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::EmbeddedDictionary;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionRegionIn>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Given a region ID from the geobase, returns an array of region IDs consisting of the passed region and all parents along the chain.
        )";
        FunctionDocumentation::Syntax syntax = "regionHierarchy(id[, geobase])";
        FunctionDocumentation::Arguments arguments = {
            {"id", "Region ID from the geobase", {"UInt32"}},
            {"geobase", "Optional. The dictionary key.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Array of region IDs consisting of the passed region and all parents along the chain", {"Array(UInt32)"}};
        FunctionDocumentation::Examples examples = {
            {
                "Get region hierarchy",
                "SELECT regionHierarchy(number::UInt32) AS arr, arrayMap(id -> regionToName(id, 'en'), arr) FROM numbers(5);",
                R"(
┌─arr────────────┬─arrayMap(lambda(tuple(id), regionToName(id, 'en')), regionHierarchy(CAST(number, 'UInt32')))─┐
│ []             │ []                                                                                           │
│ [1]            │ ['World']                                                                                    │
│ [2,10,9,1]     │ ['USA','North America','America','World']                                                    │
│ [3,2,10,9,1]   │ ['Colorado','USA','North America','America','World']                                         │
│ [4,3,2,10,9,1] │ ['Boulder County','Colorado','USA','North America','America','World']                        │
└────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┘
                )"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::EmbeddedDictionary;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionRegionHierarchy>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Given a region ID from the geobase, returns the name of the region in the specified language.
        )";
        FunctionDocumentation::Syntax syntax = "regionToName(id[, lang])";
        FunctionDocumentation::Arguments arguments = {
            {"id", "Region ID from the geobase", {"UInt32"}},
            {"lang", "Optional. Language code for the region name (e.g., 'en', 'ru'). Defaults to 'en'.", {"String"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Name of the region in the specified language, or an empty string if the region doesn't exist", {"String"}};
        FunctionDocumentation::Examples examples = {
            {
                "Get region names in English",
                "SELECT regionToName(number::UInt32,'en') FROM numbers(0,5);",
                R"(
┌─regionToName(CAST(number, 'UInt32'), 'en')─┐
│                                            │
│ World                                      │
│ USA                                        │
│ Colorado                                   │
│ Boulder County                             │
└────────────────────────────────────────────┘
                )"
            }
        };
        FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::EmbeddedDictionary;
        FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

        factory.registerFunction<FunctionRegionToName>(documentation);
    }
}

}
