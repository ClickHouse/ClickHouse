#include <gtest/gtest.h>

#include <Common/WriteOutcome.h>

using DB::AttemptsRecord;
using DB::WriteOutcome;

TEST(AttemptsRecord, OutcomeFollowsTheFacts)
{
    using Proof = AttemptsRecord::Proof;
    struct Case
    {
        AttemptsRecord record;
        WriteOutcome expected = WriteOutcome::NotApplied;
    };
    const Case cases[] = {
        {{.may_have_been_applied = false, .precondition_refused = false, .proof = Proof::None}, WriteOutcome::NotApplied},
        {{.may_have_been_applied = false, .precondition_refused = true, .proof = Proof::None}, WriteOutcome::ConditionFailed},
        {{.may_have_been_applied = true, .precondition_refused = false, .proof = Proof::None}, WriteOutcome::Unknown},
        /// The refusal can be the answer to a replay of an applied attempt.
        {{.may_have_been_applied = true, .precondition_refused = true, .proof = Proof::None}, WriteOutcome::Unknown},
        /// A proof decides, whatever the attempts were.
        {{.may_have_been_applied = true, .precondition_refused = true, .proof = Proof::Applied}, WriteOutcome::Applied},
        {{.may_have_been_applied = true, .precondition_refused = false, .proof = Proof::ConditionFailed}, WriteOutcome::ConditionFailed},
    };
    for (size_t i = 0; i < std::size(cases); ++i)
        EXPECT_EQ(cases[i].record.outcome(), cases[i].expected) << "case " << i;
}

TEST(AttemptsRecord, NewCallKeepsTheAttempts)
{
    AttemptsRecord record{.may_have_been_applied = true, .precondition_refused = true, .proof = AttemptsRecord::Proof::Applied};
    record.startCall();
    EXPECT_TRUE(record.may_have_been_applied);
    EXPECT_FALSE(record.precondition_refused);
    EXPECT_EQ(record.proof, AttemptsRecord::Proof::None);
    EXPECT_EQ(record.outcome(), WriteOutcome::Unknown);
}
