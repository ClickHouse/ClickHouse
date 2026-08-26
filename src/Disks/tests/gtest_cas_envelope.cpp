#include <gtest/gtest.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasBlobEnvelopeFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasFormat.h>
#include <string>

using namespace DB;
using namespace DB::Cas;

/// The v3 blob-envelope shape (256-byte JSON header + payload). Full round-trip / gate / pad-zone /
/// budget / critical-key coverage lives in gtest_cas_blob_envelope_format.cpp; these two keep the
/// cases that file does not exercise: a header with NO provenance/ref, and the incarnation-zone
/// independence of the payload.

TEST(CASEnvelope, BlobRoundTripNoExtensions)
{
    const std::string payload = "hello payload";
    EnvelopeHeader h;
    h.kind = ObjectKind::Blob;
    h.incarnation_tag = 0x22;
    h.build_id = 0x33;
    const std::string obj = encodeEnvelopeHeader(h, 256) + payload;

    const EnvelopeHeader d = decodeEnvelopeHeader(obj, obj.size(), ObjectKind::Blob);
    EXPECT_EQ(d.kind, ObjectKind::Blob);
    EXPECT_EQ(d.compatibility_version, G_BUILD);
    EXPECT_FALSE(d.provenance.has_value());    /// none set -> the ts/by/op/ch keys are absent
    EXPECT_FALSE(d.intended_ref.has_value());  /// none set -> the ref key is omitted
    EXPECT_EQ(d.header_len, 256u);
    /// payload starts right after the fixed-length header.
    EXPECT_EQ(obj.substr(payloadOffset(d)), payload);
}

TEST(CASEnvelope, IncarnationZoneDoesNotAffectPayload)
{
    /// Two objects with the SAME payload but DIFFERENT incarnation_tag/build_id encode to different
    /// header bytes, yet both carry the same payload at the same fixed offset — the incarnation zone
    /// never affects the payload. Identity is the content key, not any header field.
    const std::string payload = "same content";
    EnvelopeHeader a;
    a.kind = ObjectKind::Blob;
    a.incarnation_tag = 0xAAAA;
    a.build_id = 0xBBBB;
    EnvelopeHeader b = a;
    b.incarnation_tag = 0xCCCC;
    b.build_id = 0xDDDD;

    const std::string ha = encodeEnvelopeHeader(a, 256);
    const std::string hb = encodeEnvelopeHeader(b, 256);
    EXPECT_NE(ha, hb);   /// headers differ (incarnation zone)

    const EnvelopeHeader da = decodeEnvelopeHeader(ha + payload, ha.size() + payload.size(), ObjectKind::Blob);
    const EnvelopeHeader db = decodeEnvelopeHeader(hb + payload, hb.size() + payload.size(), ObjectKind::Blob);
    EXPECT_EQ((ha + payload).substr(payloadOffset(da)), payload);
    EXPECT_EQ((hb + payload).substr(payloadOffset(db)), payload);
}
