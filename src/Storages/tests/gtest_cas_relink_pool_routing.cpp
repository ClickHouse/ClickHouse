#include <gtest/gtest.h>
#include <Storages/MergeTree/DataPartsExchangeCasRouting.h>

using namespace DB::DataPartsExchange;
using DB::Strings;

/// ---- the advertise: sort, unique, ", " ----

TEST(CASRelinkPoolAdvertise, EmptySetIsEmptyStringBothWays)
{
    EXPECT_EQ(encodeCasPoolAdvertise({}), "");
    EXPECT_TRUE(decodeCasPoolAdvertise("").empty());
}

TEST(CASRelinkPoolAdvertise, SingleIdIsVerbatim)
{
    /// The one-element wire form must be byte-for-byte the pre-set-advertise form: a sender that compares
    /// the whole parameter with its own pool id must still match.
    EXPECT_EQ(encodeCasPoolAdvertise({"0123abcd"}), "0123abcd");
    EXPECT_EQ(decodeCasPoolAdvertise("0123abcd"), Strings{"0123abcd"});
}

TEST(CASRelinkPoolAdvertise, SortsAndDeduplicates)
{
    EXPECT_EQ(encodeCasPoolAdvertise({"bb", "aa", "bb", "aa"}), "aa, bb");
    EXPECT_EQ(decodeCasPoolAdvertise("aa, bb"), (Strings{"aa", "bb"}));
}

TEST(CASRelinkPoolAdvertise, DropsEmptyIds)
{
    EXPECT_EQ(encodeCasPoolAdvertise({"", "aa", ""}), "aa");
    EXPECT_EQ(encodeCasPoolAdvertise({""}), "");
}

TEST(CASRelinkPoolAdvertise, RoundTripsThreeIds)
{
    const Strings ids{"cc", "aa", "bb"};
    EXPECT_EQ(decodeCasPoolAdvertise(encodeCasPoolAdvertise(ids)), (Strings{"aa", "bb", "cc"}));
}

/// ---- which pool the offer is for ----

TEST(CASRelinkPoolAdvertise, OfferedPoolIsTheCookieWhenItWasAdvertised)
{
    EXPECT_EQ(resolveOfferedCasPool({"aa", "bb"}, "bb"), "bb");
    EXPECT_EQ(resolveOfferedCasPool({"aa"}, "aa"), "aa");
}

TEST(CASRelinkPoolAdvertise, UnadvertisedCookieIsNoPool)
{
    /// The byte re-request after a failed relink advertises nothing; an offer that arrives anyway must
    /// not re-enter the relink path through its cookie.
    EXPECT_EQ(resolveOfferedCasPool({"aa"}, "zz"), "");
    EXPECT_EQ(resolveOfferedCasPool({}, "aa"), "");
}

TEST(CASRelinkPoolAdvertise, AbsentCookieMeansTheSingleAdvertisedPool)
{
    EXPECT_EQ(resolveOfferedCasPool({"aa"}, ""), "aa");
}

TEST(CASRelinkPoolAdvertise, AbsentCookieWithSeveralPoolsIsNoPool)
{
    EXPECT_EQ(resolveOfferedCasPool({"aa", "bb"}, ""), "");
    EXPECT_EQ(resolveOfferedCasPool({}, ""), "");
}

/// ---- the receiver's forced candidate ----

static std::vector<CasRelinkCandidate> twoPools()
{
    return {
        {"disk_other", "other", false},
        {"disk_shared", "shared", false},
    };
}

TEST(CASRelinkPoolAdvertise, CookieSelectsTheCandidateOnThatPool)
{
    EXPECT_EQ(resolveForcedCaCandidate(twoPools(), {"other", "shared"}, "shared"), std::optional<size_t>{1});
    EXPECT_EQ(resolveForcedCaCandidate(twoPools(), {"other", "shared"}, "other"), std::optional<size_t>{0});
}

TEST(CASRelinkPoolAdvertise, AbsentCookieWithOneAdvertisedPoolSelectsIt)
{
    const std::vector<CasRelinkCandidate> one{{"disk_shared", "shared", false}};
    EXPECT_EQ(resolveForcedCaCandidate(one, {"shared"}, ""), std::optional<size_t>{0});
}

TEST(CASRelinkPoolAdvertise, AbsentCookieWithTwoAdvertisedPoolsSelectsNothing)
{
    EXPECT_EQ(resolveForcedCaCandidate(twoPools(), {"other", "shared"}, ""), std::nullopt);
}

TEST(CASRelinkPoolAdvertise, UnknownPoolSelectsNothing)
{
    EXPECT_EQ(resolveForcedCaCandidate(twoPools(), {"other", "shared"}, "zz"), std::nullopt);
}

TEST(CASRelinkPoolAdvertise, ReadOnlyCandidateIsSkipped)
{
    const std::vector<CasRelinkCandidate> ro{{"disk_shared_ro", "shared", true}};
    EXPECT_EQ(resolveForcedCaCandidate(ro, {"shared"}, "shared"), std::nullopt);

    const std::vector<CasRelinkCandidate> ro_then_rw{{"disk_shared_ro", "shared", true}, {"disk_shared", "shared", false}};
    EXPECT_EQ(resolveForcedCaCandidate(ro_then_rw, {"shared"}, "shared"), std::optional<size_t>{1});
}

TEST(CASRelinkPoolAdvertise, EmptyPoolIdNeverMatches)
{
    const std::vector<CasRelinkCandidate> not_started{{"disk_cold", "", false}};
    EXPECT_EQ(resolveForcedCaCandidate(not_started, {}, ""), std::nullopt);
    EXPECT_EQ(resolveForcedCaCandidate(not_started, {""}, ""), std::nullopt);
}

TEST(CASRelinkPoolAdvertise, TwoCandidatesOnOnePoolTakeTheFirst)
{
    const std::vector<CasRelinkCandidate> two{{"disk_a", "shared", false}, {"disk_b", "shared", false}};
    EXPECT_EQ(resolveForcedCaCandidate(two, {"shared"}, "shared"), std::optional<size_t>{0});
}

/// ---- the sender's confirm routing ----

static const void * const MOUNT_A = reinterpret_cast<const void *>(0x10);
static const void * const MOUNT_B = reinterpret_cast<const void *>(0x20);

TEST(CASRelinkConfirmRouting, OneOwnerAnswers)
{
    const std::vector<CasConfirmRoutingCandidate> c{{MOUNT_A, "shared", true}};
    EXPECT_EQ(resolveConfirmRoutingCandidate(c, "shared"), std::optional<size_t>{0});
}

TEST(CASRelinkConfirmRouting, NoOwnerIsNoAnswer)
{
    const std::vector<CasConfirmRoutingCandidate> c{{MOUNT_A, "shared", false}, {MOUNT_B, "other", true}};
    EXPECT_EQ(resolveConfirmRoutingCandidate(c, "shared"), std::nullopt);
    EXPECT_EQ(resolveConfirmRoutingCandidate(c, ""), std::nullopt);
    EXPECT_EQ(resolveConfirmRoutingCandidate({}, "shared"), std::nullopt);
}

TEST(CASRelinkConfirmRouting, TwoDistinctOwnersAreAmbiguous)
{
    const std::vector<CasConfirmRoutingCandidate> c{{MOUNT_A, "shared", true}, {MOUNT_B, "shared", true}};
    EXPECT_EQ(resolveConfirmRoutingCandidate(c, "shared"), std::nullopt);
}

TEST(CASRelinkConfirmRouting, AliasesOfOneMountCountOnce)
{
    /// A base disk and its cache wrapper share one exchange object: one mount, two disk names.
    const std::vector<CasConfirmRoutingCandidate> c{{MOUNT_A, "shared", true}, {MOUNT_A, "shared", true}};
    EXPECT_EQ(resolveConfirmRoutingCandidate(c, "shared"), std::optional<size_t>{0});
}

TEST(CASRelinkConfirmRouting, NonOwnerOnThePoolIsIgnored)
{
    const std::vector<CasConfirmRoutingCandidate> c{{MOUNT_A, "shared", false}, {MOUNT_B, "shared", true}};
    EXPECT_EQ(resolveConfirmRoutingCandidate(c, "shared"), std::optional<size_t>{1});
}
