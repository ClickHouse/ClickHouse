#ifndef BLAKE3_H
#define BLAKE3_H

#include <cstdint>


extern "C" {

char *blake3_apply_shim(const char *begin, uint32_t _size, uint8_t *out_char_data);

char *blake3_apply_shim_msan_compat(const char *begin, uint32_t size, uint8_t *out_char_data);

void blake3_free_char_pointer(char *ptr_to_free);

} // extern "C"

#endif /* BLAKE3_H */
