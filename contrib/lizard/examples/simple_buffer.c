/*
 * simple_buffer.c
 * Copyright  : Kyle Harper
 * License    : Follows same licensing as the lizard_compress.c/lizard_compress.h program at any given time.  Currently, BSD 2.
 * Description: Example program to demonstrate the basic usage of the compress/decompress functions within lizard_compress.c/lizard_compress.h.
 *              The functions you'll likely want are Lizard_compress_MinLevel and Lizard_decompress_safe.  Both of these are documented in
 *              the lizard_compress.h header file; I recommend reading them.
 */

/* Includes, for Power! */
#include "lizard_compress.h"    // This is all that is required to expose the prototypes for basic compression and decompression.
#include "lizard_decompress.h"
#include <stdio.h>  // For printf()
#include <string.h> // For memcmp()
#include <stdlib.h> // For exit()

/*
 * Easy show-error-and-bail function.
 */
void run_screaming(const char *message, const int code) {
  printf("%s\n", message);
  exit(code);
  return;
}


/*
 * main
 */
int main(void) {
  /* Introduction */
  // Below we will have a Compression and Decompression section to demonstrate.  There are a few important notes before we start:
  //   1) The return codes of Lizard_ functions are important.  Read lizard_compress.h if you're unsure what a given code means.
  //   2) Lizard uses char* pointers in all Lizard_ functions.  This is baked into the API and probably not going to change.  If your
  //      program uses pointers that are unsigned char*, void*, or otherwise different you may need to do some casting or set the
  //      right -W compiler flags to ignore those warnings (e.g.: -Wno-pointer-sign).

  /* Compression */
  // We'll store some text into a variable pointed to by *src to be compressed later.
  const char *src = "Lorem ipsum dolor sit amet, consectetur adipiscing elit.";
  // The compression function needs to know how many bytes of exist.  Since we're using a string, we can use strlen() + 1 (for \0).
  const size_t src_size = strlen(src) + 1;
  // Lizard provides a function that will tell you the maximum size of compressed output based on input data via Lizard_compressBound().
  const size_t max_dst_size = Lizard_compressBound(src_size);
  // We will use that size for our destination boundary when allocating space.
  char *compressed_data = malloc(max_dst_size);
  if (compressed_data == NULL)
    run_screaming("Failed to allocate memory for *compressed_data.", 1);
  // That's all the information and preparation Lizard needs to compress *src into *compressed_data.  Invoke Lizard_compress_MinLevel now
  // with our size values and pointers to our memory locations.  Save the return value for error checking.
  int return_value = 0;
  return_value = Lizard_compress_MinLevel(src, compressed_data, src_size, max_dst_size);
  // Check return_value to determine what happened.
  if (return_value < 0)
    run_screaming("A negative result from Lizard_compress_MinLevel indicates a failure trying to compress the data.  See exit code (echo $?) for value returned.", return_value);
  if (return_value == 0)
    run_screaming("A result of 0 means compression worked, but was stopped because the destination buffer couldn't hold all the information.", 1);
  if (return_value > 0)
    printf("We successfully compressed some data!\n");
  // Not only does a positive return_value mean success, the value returned == the number of bytes required.  You can use this to
  // realloc() *compress_data to free up memory, if desired.  We'll do so just to demonstrate the concept.
  const size_t compressed_data_size = return_value;
  compressed_data = (char *)realloc(compressed_data, compressed_data_size);
  if (compressed_data == NULL)
    run_screaming("Failed to re-alloc memory for compressed_data.  Sad :(", 1);

  /* Decompression */
  // Now that we've successfully compressed the information from *src to *compressed_data, let's do the opposite!  We'll create a
  // *new_src location of size src_size since we know that value.
  char *new_src = malloc(src_size);
  if (new_src == NULL)
    run_screaming("Failed to allocate memory for *new_src.", 1);
  // The Lizard_decompress_safe function needs to know where the compressed data is, how many bytes long it is, where the new_src
  // memory location is, and how large the new_src (uncompressed) output will be.  Again, save the return_value.
  return_value = Lizard_decompress_safe(compressed_data, new_src, compressed_data_size, src_size);
  if (return_value < 0)
    run_screaming("A negative result from Lizard_decompress_fast indicates a failure trying to decompress the data.  See exit code (echo $?) for value returned.", return_value);
  if (return_value == 0)
    run_screaming("I'm not sure this function can ever return 0.  Documentation in lizard_compress.h doesn't indicate so.", 1);
  if (return_value > 0)
    printf("We successfully decompressed some data!\n");
  // Not only does a positive return value mean success, the value returned == the number of bytes read from the compressed_data
  // stream.  I'm not sure there's ever a time you'll need to know this in most cases...

  /* Validation */
  // We should be able to compare our original *src with our *new_src and be byte-for-byte identical.
  if (memcmp(src, new_src, src_size) != 0)
    run_screaming("Validation failed.  *src and *new_src are not identical.", 1);
  printf("Validation done.  The string we ended up with is:\n%s\n", new_src);
  return 0;
}
