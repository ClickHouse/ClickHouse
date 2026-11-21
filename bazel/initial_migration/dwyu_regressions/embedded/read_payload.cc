#include "config.h"

extern const unsigned char dwyu_embedded_data[] = {
#embed "assets/payload.bin"
};

static_assert(sizeof(dwyu_embedded_data) == DWYU_PAYLOAD_SIZE);
