#include "api/value.h"

extern const unsigned char pts_payload[] = {
#embed "assets/payload.bin"
};

static_assert(sizeof(pts_payload) == PTS_PAYLOAD_SIZE);
