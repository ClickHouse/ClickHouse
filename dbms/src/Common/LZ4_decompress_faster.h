namespace LZ4
{

/** This method dispatch to one of different implementations depending on compression ratio.
  * 'dest' buffer must have at least 15 excessive bytes, that is allowed to overwrite with garbage.
  */
void decompress(
    const char * const source,
    char * const dest,
    size_t source_size,
    size_t dest_size);

/** Obtain statistics about LZ4 block useful for development.
  */
struct Stat
{
    size_t num_tokens = 0;
    size_t sum_literal_lengths = 0;
    size_t sum_match_lengths = 0;
    size_t sum_match_offsets = 0;
    size_t count_match_offset_less_8 = 0;
    size_t count_match_offset_less_16 = 0;
    size_t count_match_replicate_itself = 0;

    void literal(size_t length);
    void match(size_t length, size_t offset);

    void print() const;
};

Stat statistics(
    const char * const source,
    char * const dest,
    size_t dest_size,
    Stat & stat);

}
