#include <Storages/CompressionCodecSelector.h>

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>

/** The server-wide `<compression>` selector builds a codec from just its family name and level, with
  * no column type, and returns it as a part's default codec. The part writer then re-resolves that
  * stored `CODEC(...)` description with each column's type, which does not go through the
  * `allow_experimental_codecs` gate or the "requires column type" rejection that the SQL/untyped
  * codec settings enforce. So an experimental or type-dependent codec placed in `<compression>` would
  * otherwise silently bypass that gate. These tests pin the fail-closed contract: the selector rejects
  * such codecs at config load, while ordinary codecs are still accepted.
  */

using namespace DB;

namespace
{

Poco::AutoPtr<Poco::Util::XMLConfiguration> makeConfig(const std::string & xml)
{
    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    return Poco::AutoPtr<Poco::Util::XMLConfiguration>(new Poco::Util::XMLConfiguration(document));
}

std::string oneCaseConfig(const std::string & method_element)
{
    return "<clickhouse><compression><case>" + method_element + "</case></compression></clickhouse>";
}

}

TEST(CompressionCodecSelector, AcceptsOrdinaryCodecs)
{
    auto lz4 = makeConfig(oneCaseConfig("<method>lz4</method>"));
    EXPECT_NO_THROW(CompressionCodecSelector(*lz4, "compression"));

    auto zstd = makeConfig(oneCaseConfig("<method>zstd</method><level>3</level>"));
    EXPECT_NO_THROW(CompressionCodecSelector(*zstd, "compression"));

    /// An empty <compression> (no cases) always chooses the default codec and must be accepted.
    auto empty = makeConfig("<clickhouse><compression></compression></clickhouse>");
    EXPECT_NO_THROW(CompressionCodecSelector(*empty, "compression"));
}

TEST(CompressionCodecSelector, RejectsTypeDependentCodec)
{
    /// `PCO` requires the column type to compress, which the selector never has.
    auto pco = makeConfig(oneCaseConfig("<method>pco</method>"));
    EXPECT_THROW(CompressionCodecSelector(*pco, "compression"), Exception);

    /// Case-insensitive family name, and rejected even alongside an accepted case.
    auto pco_mixed = makeConfig(
        "<clickhouse><compression>"
        "<case><min_part_size>1000</min_part_size><method>lz4</method></case>"
        "<case><method>PCO</method></case>"
        "</compression></clickhouse>");
    EXPECT_THROW(CompressionCodecSelector(*pco_mixed, "compression"), Exception);
}

TEST(CompressionCodecSelector, RejectsExperimentalCodec)
{
    /// `ALP` is experimental but does not require a column type: it must still be rejected, since the
    /// selector path has no `allow_experimental_codecs` equivalent.
    auto alp = makeConfig(oneCaseConfig("<method>alp</method>"));
    EXPECT_THROW(CompressionCodecSelector(*alp, "compression"), Exception);
}
