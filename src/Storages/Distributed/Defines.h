#pragma once

/// Marks that extra information is sent to a shard. It could be any magic numbers.
#define DBMS_DISTRIBUTED_SIGNATURE_HEADER 0xCAFEDACEull
#define DBMS_DISTRIBUTED_SIGNATURE_HEADER_OLD_FORMAT 0xCAFECABEull
