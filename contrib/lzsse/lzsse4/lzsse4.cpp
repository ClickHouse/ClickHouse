/*
Copyright (c) 2016, Conor Stokes
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
this list of conditions and the following disclaimer in the documentation
and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <stddef.h>

#include "lzsse4_platform.h"
#include "lzsse4.h"

#pragma warning ( disable : 4127 )

namespace
{
    // Constants - most of these should not be changed without corresponding code changes because it will break many things in unpredictable ways.
    const uint32_t WINDOW_BITS             = 16;
    const uint32_t MIN_MATCH_LENGTH        = 4; 
    const uint32_t LZ_WINDOW_SIZE          = 1 << WINDOW_BITS;
    const uint32_t LZ_WINDOW_MASK          = LZ_WINDOW_SIZE - 1;
    const uint32_t FAST_HASH_BITS          = 20; // You can change this - more bits = more matches, less bits = more cache hits
    const uint32_t FAST_BUCKETS_COUNT      = 1 << FAST_HASH_BITS;
    const uint32_t FAST_HASH_MASK          = FAST_BUCKETS_COUNT - 1;
    const uint32_t MIN_COMPRESSION_SIZE    = 32;
    const uint32_t END_PADDING_LITERALS    = 16;
    const int32_t  NO_MATCH                = -1;
    const int32_t  EMPTY_NODE              = -1;
    const uint32_t MIN_LITERAL_COUNT       = 8;
    const uint32_t CONTROL_BITS            = 4;
    const uint32_t LITERAL_BITS            = 8;
    const uint32_t OFFSET_BITS             = 16;
    const uint32_t BASE_MATCH_BITS         = OFFSET_BITS + CONTROL_BITS;
    const uint32_t OFFSET_SIZE             = 2;
    const uint32_t EXTENDED_MATCH_BOUND    = ( 1 << CONTROL_BITS ) - 1;
    const uint32_t CONTROL_BLOCK_SIZE      = sizeof( __m128i );
    const uint32_t CONTROLS_PER_BLOCK      = 32;
    const uint32_t LITERALS_PER_CONTROL    = 4;
    const uint32_t MAX_INPUT_PER_CONTROL   = 4;
    const size_t   OUTPUT_BUFFER_SAFE      = EXTENDED_MATCH_BOUND * CONTROLS_PER_BLOCK;
    const size_t   INPUT_BUFFER_SAFE       = MAX_INPUT_PER_CONTROL * CONTROLS_PER_BLOCK;
    const uint16_t INITIAL_OFFSET          = MIN_MATCH_LENGTH;
    const uint32_t OPTIMAL_HASH_BITS       = 20;
    const uint32_t OPTIMAL_BUCKETS_COUNT   = 1 << OPTIMAL_HASH_BITS;
    const uint32_t OPTIMAL_HASH_MASK       = OPTIMAL_BUCKETS_COUNT - 1;
    const size_t   SKIP_MATCH_LENGTH       = 128;
    const uint32_t NO_SKIP_LEVEL           = 17;
}


struct LZSSE4_FastParseState
{
    int32_t buckets[ FAST_BUCKETS_COUNT ]; // stores the first matching position, we can then look at the rest of the matches by tracing through the window.
};


LZSSE4_FastParseState* LZSSE4_MakeFastParseState()
{
    return new LZSSE4_FastParseState();
}


void LZSSE4_FreeFastParseState( LZSSE4_FastParseState* toFree )
{
    delete toFree;
}


inline void SetHash( LZSSE4_FastParseState* state, uint32_t hash, const uint8_t* input, const uint8_t* inputCursor )
{
    int32_t position = static_cast<int32_t>( inputCursor - input );

    state->buckets[ hash & FAST_HASH_MASK ] = position;
}

// Simple fast hash function - actually what is used in snappy and derivatives
// There's probably better, but I haven't spent time focusing on this area yet.
inline uint32_t HashFast( const uint8_t* inputCursor )
{
    return *reinterpret_cast<const uint32_t*>( inputCursor ) * 0x1e35a7bd >> ( 32 - FAST_HASH_BITS );
}

size_t LZSSE4_CompressFast( LZSSE4_FastParseState* state, const void* inputChar, size_t inputLength, void* outputChar, size_t outputLength )
{
    if ( outputLength < inputLength )
    {
        // error case, output buffer not large enough.
        return 0;
    }

    const uint8_t* input  = reinterpret_cast< const uint8_t* >( inputChar );
    uint8_t*       output = reinterpret_cast< uint8_t* >( outputChar );

    if ( inputLength < MIN_COMPRESSION_SIZE )
    {
        memcpy( output, input, inputLength );

        return inputLength;
    }

    const uint8_t* inputCursor    = input;
    const uint8_t* inputEnd       = input + inputLength;
    const uint8_t* inputEarlyEnd  = inputEnd - END_PADDING_LITERALS;
    uint8_t*       outputCursor   = output;
    uint8_t*       outputEarlyEnd = ( output + outputLength ) - END_PADDING_LITERALS;
    uint32_t       hash           = 0;

    // initialize hash to empty 
    for ( int32_t* where = state->buckets, *end = state->buckets + FAST_BUCKETS_COUNT; where < end; where += 4 )
    {
        where[ 0 ] = -1;
        where[ 1 ] = -1;
        where[ 2 ] = -1;
        where[ 3 ] = -1;
    }

    // initial literals that wont be compressed
    for ( uint32_t where = 0; where < MIN_MATCH_LENGTH; ++where )
    {
        hash = HashFast( inputCursor );

        SetHash( state, hash, input, inputCursor );

        *( outputCursor++ ) = *( inputCursor++ );
    }

    uint8_t* currentControlBlock = outputCursor;    
    uint32_t currentControlCount = 0;
    uint16_t previousOffset      = INITIAL_OFFSET;
    size_t   literalsToFlush     = 0;

    outputCursor += CONTROL_BLOCK_SIZE;

    bool lastControlIsNop = false;
    
    // Loop through the data until we hit the end of one of the buffers (minus the end padding literals)
    while ( inputCursor < inputEarlyEnd && outputCursor <= outputEarlyEnd )
    { 
        lastControlIsNop = false;

        hash = HashFast( inputCursor );

        int      matchPosition   = state->buckets[ hash & FAST_HASH_MASK ];
        int      currentPosition = static_cast< int32_t >( inputCursor - input );
        uint32_t matchLength     = 0;
        uint16_t matchOffset     = static_cast< uint16_t >( currentPosition - matchPosition );

        // If we had a hit in the hash and it wasn't outside the window.
        if ( matchPosition >= 0 && ( currentPosition - matchPosition ) < ( LZ_WINDOW_SIZE - 1 ) )
        {
            const uint8_t* matchCandidate = input + matchPosition;
            uint32_t       lengthToEnd    = static_cast< uint32_t >( inputEarlyEnd - inputCursor );
            // Here we limit the hash length to prevent overlap matches with offset less than 16 bytes
            uint32_t       maxLength      = matchOffset <= ( EXTENDED_MATCH_BOUND + 1 ) && matchOffset < lengthToEnd ? matchOffset : lengthToEnd;

            // Find how long the match is 16 bytes at a time.
            while ( matchLength < maxLength )
            {
                __m128i input16 = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor + matchLength ) );
                __m128i match16 = _mm_loadu_si128( reinterpret_cast<const __m128i*>( matchCandidate + matchLength ) );

                unsigned long matchBytes;

                // Finds the number of equal bytes at the start of the 16 
                _BitScanForward( &matchBytes, ( static_cast< unsigned long >( ~_mm_movemask_epi8( _mm_cmpeq_epi8( input16, match16 ) ) ) | 0x10000 ) );

                matchLength += matchBytes;

                if ( matchBytes != sizeof( __m128i ) )
                {
                    break;
                }
            }

            matchLength = matchLength < maxLength ? matchLength : maxLength;
        }

        // If we have at least the minimum match length (4 bytes)
        if ( matchLength >= MIN_MATCH_LENGTH )
        {
            // Do we have literals to flush before the match?
            if ( literalsToFlush > 0 )
            {
                // Start a new control block if we need one.
                if ( currentControlCount == CONTROLS_PER_BLOCK )
                {
                    currentControlBlock  = outputCursor;
                    outputCursor        += CONTROL_BLOCK_SIZE;

                    _mm_storeu_si128( reinterpret_cast< __m128i* >( outputCursor ), _mm_setzero_si128() );

                    currentControlCount  = 0;

                    // Would be larger than compressed size, get out!
                    if ( outputCursor > outputEarlyEnd )
                    {
                        break;
                    }
                }

                currentControlBlock[ currentControlCount >> 1 ] = 
                    ( currentControlBlock[ currentControlCount >> 1 ] >> 4 ) | ( static_cast<uint8_t>( literalsToFlush - 1 ) << 4 );

                // flush the literals.
                // note the xor against the data that would be read in the match.
                for ( uint32_t where = 0; where < literalsToFlush; ++where )
                {
                    const uint8_t* currentInput = inputCursor - ( literalsToFlush - where );

                    *( outputCursor++ ) = *currentInput ^ *( currentInput - previousOffset );
                }

                ++currentControlCount;

                literalsToFlush = 0;
                    
                // Would be larger than compressed size, get out!
                if ( outputCursor > outputEarlyEnd )
                {
                    break;
                }
            }

            // Start a new control block if the previous one is full.
            // Note this is done before the offset is written out - the offset
            // is always written after the control block containing the first
            // control in the match.
            if ( currentControlCount == CONTROLS_PER_BLOCK )
            {
                currentControlBlock  = outputCursor;
                outputCursor        += CONTROL_BLOCK_SIZE;

                _mm_storeu_si128( reinterpret_cast< __m128i* >( outputCursor ), _mm_setzero_si128() );

                currentControlCount  = 0;
                    
                if ( outputCursor > outputEarlyEnd )
                {
                    break;
                }
            }

            // The match length value we are encoding.
            size_t toEncode = matchLength;

            // Write the offset out - note the xor with the previous offset.
            *reinterpret_cast< uint16_t* >( outputCursor ) = matchOffset ^ previousOffset;

            previousOffset = matchOffset;
            outputCursor  += sizeof( uint16_t );

            for ( ;; )
            {
                // Check if we need to start a new control block
                if ( currentControlCount == CONTROLS_PER_BLOCK )
                {
                    currentControlBlock  = outputCursor;
                    outputCursor        += CONTROL_BLOCK_SIZE;

                    _mm_storeu_si128( reinterpret_cast< __m128i* >( outputCursor ), _mm_setzero_si128() );

                    currentControlCount  = 0;
                        
                    if ( outputCursor > outputEarlyEnd )
                    {
                        break;
                    }
                }

                // If the encode size is greater than we can hold in a control, write out a full match length
                // control, subtract full control value from the amount to encode and loop around again.
                if ( toEncode >= EXTENDED_MATCH_BOUND )
                {
                    currentControlBlock[ currentControlCount >> 1 ] = 
                        ( currentControlBlock[ currentControlCount >> 1 ] >> 4 ) | ( static_cast<uint8_t>( EXTENDED_MATCH_BOUND ) << 4 );

                    toEncode -= EXTENDED_MATCH_BOUND;

                    ++currentControlCount;
                }
                else // Write out the remaining match length control. Could potentially be zero.
                {
                    currentControlBlock[ currentControlCount >> 1 ] = 
                        ( currentControlBlock[ currentControlCount >> 1 ] >> 4 ) | ( static_cast<uint8_t>( toEncode ) << 4 );

                    if ( currentControlCount == 0 && toEncode == 0 )
                    {
                        lastControlIsNop = true;
                    }

                    ++currentControlCount;

                    break;
                }
            }

            // Update the value into the hash for future matches.
            SetHash( state, hash, input, inputCursor );

            ++inputCursor;

            // Hash all the other values in the match too.
            for ( const uint8_t* nextArrival = inputCursor + matchLength - 1; inputCursor < nextArrival; ++inputCursor )
            {
                hash = HashFast( inputCursor );
                SetHash( state, hash, input, inputCursor );
            }
        }
        else
        {
            // One more literal to write out.
            ++literalsToFlush;

            // If we have reached the maximum number of literals allowed in the control, flush them out.
            if ( literalsToFlush == LITERALS_PER_CONTROL )
            {
                // Check if the control block is full and we need start a new one.
                if ( currentControlCount == CONTROLS_PER_BLOCK )
                {
                    currentControlBlock  = outputCursor;
                    outputCursor        += CONTROL_BLOCK_SIZE;

                    _mm_storeu_si128( reinterpret_cast< __m128i* >( outputCursor ), _mm_setzero_si128() );

                    currentControlCount  = 0;

                    if ( outputCursor > outputEarlyEnd )
                    {
                        break;
                    }
                }

                currentControlBlock[ currentControlCount >> 1 ] = 
                    ( currentControlBlock[ currentControlCount >> 1 ] >> 4 ) | ( ( static_cast<uint8_t>( LITERALS_PER_CONTROL - 1 ) ) << 4 );

                ++currentControlCount;

                *reinterpret_cast< uint32_t* >( outputCursor ) = 
                    *reinterpret_cast< const uint32_t* >( inputCursor - 3 ) ^ 
                    *reinterpret_cast< const uint32_t* >( ( inputCursor - 3 ) - previousOffset );

                outputCursor += 4;

                //*( outputCursor++ ) = *( inputCursor - 3 ) ^ *( ( inputCursor - 3 ) - previousOffset );
                //*( outputCursor++ ) = *( inputCursor - 2 ) ^ *( ( inputCursor - 2 ) - previousOffset );
                //*( outputCursor++ ) = *( inputCursor - 1 ) ^ *( ( inputCursor - 1 ) - previousOffset );
                //*( outputCursor++ ) = *inputCursor ^ *( inputCursor - previousOffset );
                    
                if ( outputCursor > outputEarlyEnd )
                {
                    break;
                }

                literalsToFlush = 0;
            }

            // Update the hash with this byte
            SetHash( state, hash, input, inputCursor );

            ++inputCursor;
        }
    }

    // If we would create a compression output bigger than or equal to the input, just copy the input to the output and return equal size.
    if ( ( ( outputCursor + literalsToFlush + ( currentControlCount == CONTROLS_PER_BLOCK ? CONTROL_BLOCK_SIZE : 0 ) ) ) >= output + inputLength - END_PADDING_LITERALS )
    {
        memcpy( output, input, inputLength );

        outputCursor = output + inputLength;
    }
    else 
    {
        // Flush any remaining literals.
        if ( literalsToFlush > 0 )
        {
            lastControlIsNop = false;

            if ( currentControlCount == CONTROLS_PER_BLOCK )
            {
                currentControlBlock = outputCursor;
                outputCursor       += CONTROL_BLOCK_SIZE;

                _mm_storeu_si128( reinterpret_cast< __m128i* >( outputCursor ), _mm_setzero_si128() );

                currentControlCount = 0;
            }

            currentControlBlock[ currentControlCount >> 1 ] = 
                ( currentControlBlock[ currentControlCount >> 1 ] >> 4 ) | ( static_cast<uint8_t>( literalsToFlush - 1 ) << 4 );
            
            for ( uint32_t where = 0; where < literalsToFlush; ++where )
            {
                const uint8_t* currentInput = inputCursor - ( literalsToFlush - where );

                *( outputCursor++ ) = *currentInput ^ *( currentInput - previousOffset );
            }

            ++currentControlCount;
        }

        // Need to finish off shifting the final control block into the low nibble if there is no second nibble
        if ( ( currentControlCount & 1 ) > 0 )
        {
            currentControlBlock[ currentControlCount >> 1 ] >>= 4;
        }

        if ( lastControlIsNop )
        {
            outputCursor -= CONTROL_BLOCK_SIZE;
        }

        size_t remainingLiterals = ( input + inputLength ) - inputCursor;

        // copy remaining literals
        memcpy( outputCursor, inputCursor, remainingLiterals );

        outputCursor += remainingLiterals;
    }

    // Return the size of the compressed data.
    return outputCursor - output;
}


struct Arrival
{
    size_t    cost;
    int32_t   from;
    int32_t   to;
    uint16_t  offset;
};

struct TreeNode
{
    int32_t children[ 2 ];
};

struct LZSSE4_OptimalParseState
{
    // Note, we should really replace this with a BST, hash chaining works but is *slooooooooooooooow* for optimal parse.
    int32_t roots[ OPTIMAL_BUCKETS_COUNT ];

    TreeNode window[ LZ_WINDOW_SIZE ];

    Arrival* arrivals;

    size_t bufferSize;
};


LZSSE4_OptimalParseState* LZSSE4_MakeOptimalParseState( size_t bufferSize )
{
    if ( bufferSize > 0 && ( SIZE_MAX / sizeof( Arrival ) ) < bufferSize )
    {
        return nullptr;
    }

    LZSSE4_OptimalParseState* result = reinterpret_cast< LZSSE4_OptimalParseState* >( ::malloc( sizeof( LZSSE4_OptimalParseState ) ) );

    result->bufferSize = bufferSize;

    if ( result != nullptr )
    {
        result->arrivals = reinterpret_cast< Arrival* >( ::malloc( sizeof( Arrival ) * bufferSize ) );

        if ( result->arrivals == nullptr )
        {
            LZSSE4_FreeOptimalParseState( result );

            result = nullptr;
        }
    }

    return result;
}


void LZSSE4_FreeOptimalParseState( LZSSE4_OptimalParseState* toFree )
{
    ::free( toFree->arrivals );

    toFree->arrivals = nullptr;

    ::free( toFree );
}


inline uint32_t HashOptimal( const uint8_t* inputCursor )
{
    return *reinterpret_cast<const uint32_t*>( inputCursor ) * 0x1e35a7bd >> ( 32 - OPTIMAL_HASH_BITS );
}


struct Match
{
    size_t length;
    int32_t position;
    uint16_t offset;
};


inline Match SearchAndUpdateFinder( LZSSE4_OptimalParseState& state, const uint8_t* input, const uint8_t* inputCursor, const uint8_t* inputEnd, uint32_t cutOff )
{
    Match result;

    int32_t position = static_cast<int32_t>( inputCursor - input );

    result.position = NO_MATCH;
    result.length   = MIN_MATCH_LENGTH;
    result.offset   = 0;

    size_t   lengthToEnd  = inputEnd - inputCursor;
    int32_t  lastPosition = position - ( LZ_WINDOW_SIZE - 1 );
    uint32_t hash         = HashOptimal( inputCursor );

    lastPosition = lastPosition > 0 ? lastPosition : 0;

    int32_t treeCursor = state.roots[ hash ];

    state.roots[ hash ] = position;

    int32_t* left        = &state.window[ position & LZ_WINDOW_MASK ].children[ 1 ];
    int32_t* right       = &state.window[ position & LZ_WINDOW_MASK ].children[ 0 ];
    size_t   leftLength  = 0;
    size_t   rightLength = 0;

    for ( ;; )
    {
        if ( cutOff-- == 0 || treeCursor < lastPosition )
        {
            *left = *right = EMPTY_NODE;
            break;
        }

        TreeNode&      currentNode = state.window[ treeCursor & LZ_WINDOW_MASK ];
        const uint8_t* key         = input + treeCursor;
        size_t         matchLength = leftLength < rightLength ? leftLength : rightLength;

        uint16_t       matchOffset = static_cast< uint16_t >( position - treeCursor );
        size_t         maxLength   = matchOffset <= ( EXTENDED_MATCH_BOUND + 1 ) && matchOffset < lengthToEnd ? matchOffset : lengthToEnd;

        while ( matchLength < lengthToEnd )
        {
            __m128i input16 = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor + matchLength ) );
            __m128i match16 = _mm_loadu_si128( reinterpret_cast<const __m128i*>( key + matchLength ) );

            unsigned long matchBytes;

            _BitScanForward( &matchBytes, ( static_cast<unsigned long>( ~_mm_movemask_epi8( _mm_cmpeq_epi8( input16, match16 ) ) ) | 0x10000 ) );

            matchLength += matchBytes;

            if ( matchBytes != 16 )
            {
                break;
            }
        }

        matchLength = matchLength < lengthToEnd ? matchLength : lengthToEnd;

        size_t truncatedMatchLength = matchLength < maxLength ? matchLength : maxLength;

        if ( truncatedMatchLength >= result.length && matchOffset >= LITERALS_PER_CONTROL )
        {
            result.length   = truncatedMatchLength;
            result.offset   = matchOffset;
            result.position = treeCursor;
        }

        if ( matchLength == lengthToEnd )
        {
            *left  = currentNode.children[ 1 ];
            *right = currentNode.children[ 0 ];
            break;
        }

        if ( inputCursor[ matchLength ] < key[ matchLength ] || ( matchLength == lengthToEnd ) )
        {
            *left       = treeCursor;
            left        = currentNode.children;
            treeCursor  = *left;
            leftLength  = matchLength;
        }
        else
        {
            *right      = treeCursor;
            right       = currentNode.children + 1;
            treeCursor  = *right;
            rightLength = matchLength;
        }
    }

    // Special RLE overlapping match case, the LzFind style match above doesn't work very well with our
    // restriction of overlapping matches having offsets of at least 16.
    // Suffix array seems like a better option to handling this.
    {
        // Note, we're detecting long RLE here, but if we have an offset too close, we'll sacrifice a fair 
        // amount of decompression performance to load-hit-stores.
        int32_t matchPosition = position - ( sizeof( __m128i ) * 2 );

        if ( matchPosition >= 0 )
        {
            uint16_t       matchOffset = static_cast<uint16_t>( position - matchPosition );
            const uint8_t* key = input + matchPosition;
            size_t         matchLength = 0;

            while ( matchLength < lengthToEnd )
            {
                __m128i input16 = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor + matchLength ) );
                __m128i match16 = _mm_loadu_si128( reinterpret_cast<const __m128i*>( key + matchLength ) );

                unsigned long matchBytes;

                _BitScanForward( &matchBytes, ( static_cast<unsigned long>( ~_mm_movemask_epi8( _mm_cmpeq_epi8( input16, match16 ) ) ) | 0x10000 ) );

                matchLength += matchBytes;

                if ( matchBytes != 16 )
                {
                    break;
                }

            }

            matchLength = matchLength < lengthToEnd ? matchLength : lengthToEnd;

            if ( matchLength >= result.length )
            {
                result.length = matchLength;
                result.offset = matchOffset;
                result.position = matchPosition;
            }
        }
    }

    return result;
}


size_t LZSSE4_CompressOptimalParse( LZSSE4_OptimalParseState* state, const void* inputChar, size_t inputLength, void* outputChar, size_t outputLength, unsigned int level )
{
    if ( outputLength < inputLength || state->bufferSize < inputLength )
    {
        // error case, output buffer not large enough.
        return 0;
    }

    const uint8_t* input  = reinterpret_cast< const uint8_t* >( inputChar );
    uint8_t*       output = reinterpret_cast< uint8_t* >( outputChar );

    if ( inputLength < MIN_COMPRESSION_SIZE )
    {
        memcpy( output, input, inputLength );

        return inputLength;
    }

    const uint8_t* inputCursor      = input;
    const uint8_t* inputEnd         = input + inputLength;
    Arrival*       arrivalWatermark = state->arrivals;
    Arrival*       arrival          = state->arrivals;
    uint32_t       cutOff           = 1 << level;

    for ( int32_t* rootCursor = state->roots, *end = rootCursor + OPTIMAL_BUCKETS_COUNT; rootCursor < end; rootCursor += 4 )
    {
        rootCursor[ 0 ] = EMPTY_NODE;
        rootCursor[ 1 ] = EMPTY_NODE;
        rootCursor[ 2 ] = EMPTY_NODE;
        rootCursor[ 3 ] = EMPTY_NODE;
    }

    for ( uint32_t where = 0; where < MIN_MATCH_LENGTH; ++where )
    {
        SearchAndUpdateFinder( *state, input, inputCursor, inputEnd - END_PADDING_LITERALS, cutOff );

        ++inputCursor;
    }

    arrival->cost   = LITERAL_BITS * LITERALS_PER_CONTROL;
    arrival->from   = -1;
    arrival->offset = 0;

    // loop through each character and project forward the matches at that character to calculate the cheapest
    // path of arrival for each individual character.
    for ( const uint8_t* earlyEnd = inputEnd - END_PADDING_LITERALS; inputCursor < earlyEnd; ++inputCursor, ++arrival )
    {
        uint32_t  lengthToEnd     = static_cast< uint32_t >( earlyEnd - inputCursor );       
        int32_t   currentPosition = static_cast< int32_t >( inputCursor - input );
        size_t    literalsForward = LITERALS_PER_CONTROL < lengthToEnd ? LITERALS_PER_CONTROL : lengthToEnd;
        size_t    arrivalCost     = arrival->cost;

        // NOTE - we currently assume only 2 literals filled in here, because the minimum match length is 3.
        // If we wanted to go with a higher minimum match length, we would need to fill in more literals before hand.
        // Also, because there is a maximum of 2 literals per control block assumed.

        // project forward the cost of a single literal

        for ( size_t where = 1; where <= literalsForward; ++where )
        {
            Arrival* literalArrival = arrival + where;
            size_t   literalCost    = arrivalCost + CONTROL_BITS + ( where * LITERAL_BITS );

            if ( literalArrival > arrivalWatermark || literalArrival->cost > literalCost )
            {
                literalArrival->cost   = literalCost;
                literalArrival->from   = currentPosition;
                literalArrival->offset = 0;

                arrivalWatermark = literalArrival > arrivalWatermark ? literalArrival : arrivalWatermark;
            }
        }

        Match match = SearchAndUpdateFinder( *state, input, inputCursor, earlyEnd, cutOff );

        if ( match.position != NO_MATCH )
        {
            for ( size_t matchedLength = MIN_MATCH_LENGTH, end = match.length + 1; matchedLength < end; ++matchedLength )
            {
                Arrival* matchArrival = arrival + matchedLength;
                size_t   matchCost    = arrivalCost + BASE_MATCH_BITS;

                if ( matchedLength >= EXTENDED_MATCH_BOUND )
                {
                    matchCost += ( matchedLength / EXTENDED_MATCH_BOUND ) * CONTROL_BITS;
                }

                if ( matchArrival > arrivalWatermark || matchArrival->cost > matchCost )
                {
                    matchArrival->cost   = matchCost;
                    matchArrival->from   = currentPosition;
                    matchArrival->offset = match.offset;

                    arrivalWatermark = matchArrival > arrivalWatermark ? matchArrival : arrivalWatermark;
                }
            }

            if ( match.length > SKIP_MATCH_LENGTH && level < NO_SKIP_LEVEL )
            {
                arrival     += match.length - LITERALS_PER_CONTROL;
                inputCursor += match.length - LITERALS_PER_CONTROL;
            }
        }
    }

    // If this would cost more to encode than it would if it were just literals, encode it with no control blocks,
    // just literals
    if ( ( arrivalWatermark->cost + END_PADDING_LITERALS * LITERAL_BITS + CONTROLS_PER_BLOCK * CONTROL_BITS ) > ( inputLength * LITERAL_BITS ) )
    {
        memcpy( output, input, inputLength );

        return inputLength;
    }

    Arrival* previousPathNode;

    // now trace the actual optimal parse path back, connecting the nodes in the other direction.
    for ( const Arrival* pathNode = arrivalWatermark; pathNode->from > 0; pathNode = previousPathNode )
    {
        previousPathNode     = state->arrivals + ( pathNode->from - LITERALS_PER_CONTROL );

        previousPathNode->to = static_cast<int32_t>( ( pathNode - state->arrivals ) + LITERALS_PER_CONTROL );
    }

    uint8_t* outputCursor = output;

    memcpy( outputCursor, input, MIN_MATCH_LENGTH );

    outputCursor += MIN_MATCH_LENGTH;

    uint8_t* currentControlBlock = outputCursor;
    uint32_t currentControlCount = 0;
    uint32_t totalControlCount   = 0;

    outputCursor += CONTROL_BLOCK_SIZE;

    Arrival* nextPathNode;

    size_t   totalPathLength = MIN_MATCH_LENGTH;
    uint16_t previousOffset  = INITIAL_OFFSET;

    bool lastControlIsNop = false;

    // Now walk forwards again and actually write out the data.
    for ( const Arrival* pathNode = state->arrivals; pathNode < arrivalWatermark; pathNode = nextPathNode )
    {
        int32_t currentPosition = static_cast< int32_t >( ( pathNode - state->arrivals ) + LITERALS_PER_CONTROL );

        nextPathNode = state->arrivals + ( pathNode->to - LITERALS_PER_CONTROL );

        size_t pathDistance = nextPathNode - pathNode;

        totalPathLength += pathDistance;

        lastControlIsNop = false;

        if ( nextPathNode->offset == 0 )
        {
            if ( currentControlCount == CONTROLS_PER_BLOCK )
            {
                currentControlBlock  = outputCursor;
                outputCursor        += CONTROL_BLOCK_SIZE;
                currentControlCount  = 0;
            }

            if ( ( currentControlCount & 1 ) == 0 )
            {
                currentControlBlock[ currentControlCount >> 1 ] =
                    ( static_cast<uint8_t>( pathDistance ) - 1 );
            }
            else
            {
                currentControlBlock[ currentControlCount >> 1 ] |= 
                    ( static_cast< uint8_t >( pathDistance ) - 1 ) << CONTROL_BITS;
            }

            // output the literals.
            for ( int32_t where = 0; where < pathDistance; ++where )
            {
                const uint8_t* currentInput = input + currentPosition + where;

                outputCursor[ where ] = *currentInput ^ *( currentInput - previousOffset );
            }

            outputCursor += pathDistance;

            ++totalControlCount;
            ++currentControlCount;
        }
        else
        {
            // Check if we need to start a new control block
            if ( currentControlCount == CONTROLS_PER_BLOCK )
            {
                currentControlBlock = outputCursor;
                outputCursor       += CONTROL_BLOCK_SIZE;

                _mm_storeu_si128( reinterpret_cast<__m128i*>( outputCursor ), _mm_setzero_si128() );

                currentControlCount = 0;
            }

            // Write the offset out - note the xor with the previous offset.
            *reinterpret_cast< uint16_t* >( outputCursor ) = nextPathNode->offset ^ previousOffset;

            previousOffset = nextPathNode->offset;
            outputCursor  += sizeof( uint16_t );

            if ( pathDistance < EXTENDED_MATCH_BOUND )
            {
                if ( ( currentControlCount & 1 ) == 0 )
                {
                    currentControlBlock[ currentControlCount >> 1 ] =
                        static_cast<uint8_t>( pathDistance );
                }
                else
                {
                    currentControlBlock[ currentControlCount >> 1 ] |= 
                        static_cast< uint8_t >( pathDistance ) << CONTROL_BITS;
                }

                ++currentControlCount;
            }
            else
            {
                if ( ( currentControlCount & 1 ) == 0 )
                {
                    currentControlBlock[ currentControlCount >> 1 ] =
                        static_cast<uint8_t>( EXTENDED_MATCH_BOUND );
                }
                else
                {
                    currentControlBlock[ currentControlCount >> 1 ] |= 
                        static_cast< uint8_t >( EXTENDED_MATCH_BOUND ) << CONTROL_BITS;
                }

                ++currentControlCount;

                size_t toEncode = pathDistance - EXTENDED_MATCH_BOUND;

                for ( ;; )
                {
                    // Check if we need to start a new control block
                    if ( currentControlCount == CONTROLS_PER_BLOCK )
                    {
                        currentControlBlock = outputCursor;
                        outputCursor       += CONTROL_BLOCK_SIZE;

                        _mm_storeu_si128( reinterpret_cast<__m128i*>( outputCursor ), _mm_setzero_si128() );

                        currentControlCount = 0;
                    }

                    // If the encode size is greater than we can hold in a control, write out a full match length
                    // control, subtract full control value from the amount to encode and loop around again.
                    if ( toEncode >= EXTENDED_MATCH_BOUND )
                    {
                        if ( ( currentControlCount & 1 ) == 0 )
                        {
                            currentControlBlock[ currentControlCount >> 1 ] =
                                static_cast<uint8_t>( EXTENDED_MATCH_BOUND );
                        }
                        else
                        {
                            currentControlBlock[ currentControlCount >> 1 ] |= 
                                static_cast< uint8_t >( EXTENDED_MATCH_BOUND ) << CONTROL_BITS;
                        }

                        toEncode -= EXTENDED_MATCH_BOUND;

                        ++currentControlCount;
                    }
                    else // Write out the remaining match length control. Could potentially be zero.
                    {
                        if ( ( currentControlCount & 1 ) == 0 )
                        {
                            currentControlBlock[ currentControlCount >> 1 ] =
                                static_cast<uint8_t>( toEncode );
                        }
                        else
                        {
                            currentControlBlock[ currentControlCount >> 1 ] |= 
                                static_cast< uint8_t >( toEncode ) << CONTROL_BITS;
                        }

                        if ( toEncode == 0 && currentControlCount == 0 )
                        {
                            lastControlIsNop = true;
                        }

                        ++currentControlCount;

                        break;
                    }
                }
            }
        }
    }

    if ( lastControlIsNop )
    {
        outputCursor -= CONTROL_BLOCK_SIZE;
    }

    size_t remainingLiterals = ( input + inputLength ) - inputCursor;

    // copy remaining literals
    memcpy( outputCursor, inputCursor, remainingLiterals );

    outputCursor += remainingLiterals;

    return outputCursor - output;
}


size_t LZSSE4_Decompress( const void* inputChar, size_t inputLength, void* outputChar, size_t outputLength )
{
    const uint8_t* input  = reinterpret_cast< const uint8_t* >( inputChar );
    uint8_t*       output = reinterpret_cast< uint8_t* >( outputChar );

    // Data was not compressible, just copy initial values
    if ( outputLength == inputLength )
    {
        memcpy( output, input, outputLength );

        return inputLength;
    }

    const uint8_t* inputCursor  = input;
    uint8_t*       outputCursor = output;

    // The offset starts off as the minimum match length. We actually need it least four
    // characters back because we need them to be set to xor out the literals from the match data.
    size_t  offset          = INITIAL_OFFSET;
    __m128i previousCarryHi = _mm_setzero_si128();

    // Copy the initial literals to the output.
    for ( uint32_t where = 0; where < MIN_MATCH_LENGTH; ++where )
    {
        *( outputCursor++ ) = *( inputCursor++ );
    }

    // Let me be clear, I am usually anti-macro, but they work for this particular (very unusual) case.  
    // DECODE_STEP is a regular decoding step, DECODE_STEP_HALF and DECODE_STEP_END are because the compiler couldn't
    // seem to remove some of the dead code where values were updated and then never used.

    // What these macros do:
    //     Decode a single literal run or match run for a single control nibble.
    // How they do it:
    //    - Read the *unaligned* input (in the case of LZSSE-F - twice), it goes into both a regular variable and an SSE register,
    //      because it could either be literals or an offset (or nothing at all). The low byte of streamBytesRead controls how much we advance
    //      the input cursor.
    //    - Used a contived set of casts to sign extend the "read offset" control mask and then use it to mask the input word,
    //      which is then xor'd against the offset, for a "branchless" conditional move into the offset which
    //      has been carried over from the previous literal/match block. Note, this ends up doing better than a cmov on most 
    //      modern processors. But we need to pre-xor the input offset.
    //    - We then load the match data from output buffer (offset back from the current output point). Unconditional load here.
    //    - We broadcast the "from literal" control mask from the current least significant byte of the SSE register using a shuffle epi-8
    //    - We mask the literals with that SSE register wide mask.
    //    - The literals have been pre-xor'd with the data read in as match data, so we use an xor to branchlessly choose between the two.
    //      In this case, it ends up a better option than a blendv on most processors.
    //    - Store the block. We store all 16 bytes of the SSE register (due to some constraints in the format of the data, we won't
    //      go past the end of the buffer), but we may overlap this.
    //    - bytesOut controls how much we advance the output cursor.
    //    - We use 8 bit shifts to advance all the controls up to the next byte. There is some variable sized register trickery that 
    //      x86/x64 is great for as long as we don't anger the register renamer.

#define DECODE_STEP( HILO, CHECKMATCH, CHECKBUFFERS )                                                                           \
    {                                                                                                                           \
        size_t  inputWord = *reinterpret_cast<const uint16_t*>( inputCursor );                                                  \
        __m128i literals  = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor ) );                                 \
                                                                                                                                \
        offset ^= static_cast<size_t>( static_cast<ptrdiff_t>( static_cast<int8_t>( readOffsetHalf##HILO ) ) ) & inputWord;     \
                                                                                                                                \
        readOffsetHalf##HILO >>= 8;                                                                                             \
                                                                                                                                \
        const uint8_t* matchPointer = reinterpret_cast<const uint8_t*>( outputCursor - offset );                                \
                                                                                                                                \
        if ( CHECKMATCH && matchPointer < output )                                                                              \
            goto MATCH_UNDERFLOW;                                                                                               \
                                                                                                                                \
        __m128i fromLiteral = _mm_shuffle_epi8( fromLiteral##HILO, _mm_setzero_si128() );                                       \
        __m128i matchData   = _mm_loadu_si128( reinterpret_cast<const __m128i*>( matchPointer ) );                              \
                                                                                                                                \
        literals = _mm_and_si128( literals, fromLiteral );                                                                      \
                                                                                                                                \
        fromLiteral##HILO   = _mm_srli_si128( fromLiteral##HILO, 1 );                                                           \
                                                                                                                                \
        __m128i toStore     = _mm_xor_si128( matchData, literals );                                                             \
                                                                                                                                \
        _mm_storeu_si128( reinterpret_cast<__m128i*>( outputCursor ), toStore );                                                \
                                                                                                                                \
        outputCursor += static_cast< uint8_t >( bytesOutHalf##HILO );                                                           \
        inputCursor  += static_cast< uint8_t >( streamBytesReadHalf##HILO );                                                    \
                                                                                                                                \
        bytesOutHalf##HILO        >>= 8;                                                                                        \
        streamBytesReadHalf##HILO >>= 8;                                                                                        \
                                                                                                                                \
        if ( CHECKBUFFERS && ( outputCursor >= outputEarlyEnd || inputCursor > inputEarlyEnd ) )                                \
            goto BUFFER_END;                                                                                                    \
    }

#define DECODE_STEP_HALF( HILO, CHECKMATCH, CHECKBUFFERS )                                                                      \
    {                                                                                                                           \
        size_t  inputWord = *reinterpret_cast<const uint16_t*>( inputCursor );                                                  \
        __m128i literals = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor ) );                                  \
                                                                                                                                \
        offset ^= static_cast<size_t>( static_cast<ptrdiff_t>( static_cast<int8_t>( readOffsetHalf##HILO ) ) ) & inputWord;     \
                                                                                                                                \
        const uint8_t* matchPointer = reinterpret_cast<const uint8_t*>( outputCursor - offset );                                \
                                                                                                                                \
        if ( CHECKMATCH && matchPointer < output )                                                                              \
            goto MATCH_UNDERFLOW;                                                                                               \
                                                                                                                                \
        __m128i fromLiteral = _mm_shuffle_epi8( fromLiteral##HILO, _mm_setzero_si128() );                                       \
        __m128i matchData   = _mm_loadu_si128( reinterpret_cast<const __m128i*>( matchPointer ) );                              \
                                                                                                                                \
        literals = _mm_and_si128( literals, fromLiteral );                                                                      \
                                                                                                                                \
        fromLiteral##HILO   = _mm_srli_si128( fromLiteral##HILO, 1 );                                                           \
                                                                                                                                \
        __m128i toStore     = _mm_xor_si128( matchData, literals );                                                             \
                                                                                                                                \
        _mm_storeu_si128( reinterpret_cast<__m128i*>( outputCursor ), toStore );                                                \
                                                                                                                                \
        outputCursor += static_cast< uint8_t >( bytesOutHalf##HILO );                                                           \
        inputCursor  += static_cast< uint8_t >( streamBytesReadHalf##HILO );                                                    \
                                                                                                                                \
        if ( CHECKBUFFERS && ( outputCursor >= outputEarlyEnd || inputCursor > inputEarlyEnd ) )                                \
            goto BUFFER_END;                                                                                                    \
    }

#define DECODE_STEP_END( HILO, CHECKMATCH, CHECKBUFFERS )                                                                       \
    {                                                                                                                           \
        size_t  inputWord = *reinterpret_cast<const uint16_t*>( inputCursor );                                                  \
        __m128i literals = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor ) );                                  \
                                                                                                                                \
        offset ^= static_cast<size_t>( static_cast<ptrdiff_t>( static_cast<int8_t>( readOffsetHalf##HILO ) ) ) & inputWord;     \
                                                                                                                                \
        const uint8_t* matchPointer = reinterpret_cast<const uint8_t*>( outputCursor - offset );                                \
                                                                                                                                \
        if ( CHECKMATCH && matchPointer < output )                                                                              \
            goto MATCH_UNDERFLOW;                                                                                               \
                                                                                                                                \
        __m128i fromLiteral = _mm_shuffle_epi8( fromLiteral##HILO, _mm_setzero_si128() );                                       \
        __m128i matchData   = _mm_loadu_si128( reinterpret_cast<const __m128i*>( matchPointer ) );                              \
                                                                                                                                \
        literals = _mm_and_si128( literals, fromLiteral );                                                                      \
                                                                                                                                \
        __m128i toStore     = _mm_xor_si128( matchData, literals );                                                             \
                                                                                                                                \
        _mm_storeu_si128( reinterpret_cast<__m128i*>( outputCursor ), toStore );                                                \
                                                                                                                                \
        outputCursor += static_cast< uint8_t >( bytesOutHalf##HILO );                                                           \
        inputCursor  += static_cast< uint8_t >( streamBytesReadHalf##HILO );                                                    \
                                                                                                                                \
        if ( CHECKBUFFERS && ( outputCursor >= outputEarlyEnd || inputCursor > inputEarlyEnd ) )                                \
            goto BUFFER_END;                                                                                                    \
        } 

#define DECODE_STEP_LO(CHECKMATCH, CHECKBUFFERS )          DECODE_STEP( Lo, CHECKMATCH, CHECKBUFFERS )
#define DECODE_STEP_HI(CHECKMATCH, CHECKBUFFERS )          DECODE_STEP( Hi, CHECKMATCH, CHECKBUFFERS )
#define DECODE_STEP_HALF_LO(CHECKMATCH, CHECKBUFFERS )     DECODE_STEP_HALF( Lo, CHECKMATCH, CHECKBUFFERS )
#define DECODE_STEP_HALF_HI(CHECKMATCH, CHECKBUFFERS )     DECODE_STEP_HALF( Hi, CHECKMATCH, CHECKBUFFERS )
#define DECODE_STEP_END_LO(CHECKMATCH, CHECKBUFFERS )      DECODE_STEP_END( Lo, CHECKMATCH, CHECKBUFFERS )
#define DECODE_STEP_END_HI(CHECKMATCH, CHECKBUFFERS )      DECODE_STEP_END( Hi, CHECKMATCH, CHECKBUFFERS )

    __m128i nibbleMask = _mm_set1_epi8( 0xF );
    __m128i offsetSize = _mm_set1_epi8( OFFSET_SIZE );

    // Note, we use this block here because it allows the "fake" inputEarlyEnd/outputEarlyEnd not to cause register spills 
    // in the decompression loops. And yes, that did actually happen.
    {

#pragma warning ( push )
#pragma warning ( disable : 4101 )

        // These variables are not actually ever used in this block, because we use
        // a constant conditional expression to take out the branches that would hit them.
        // But unfortunately, we need them to compile.
        const  uint8_t* inputEarlyEnd;
        uint8_t*        outputEarlyEnd;

#pragma warning ( pop )

        // "Safe" ends to the buffer, before the input/output cursors hit these, we can loop without overflow checks.
        const  uint8_t* inputSafeEnd  = ( input + inputLength ) - INPUT_BUFFER_SAFE;
        uint8_t*        outputSafeEnd = ( output + outputLength ) - OUTPUT_BUFFER_SAFE;

        // Decoding loop with offset output buffer underflow test, but no buffer overflow tests, assumed to end at a safe distance 
        // from overflows
        while ( ( outputCursor - output ) < LZ_WINDOW_SIZE && outputCursor < outputSafeEnd && inputCursor < inputSafeEnd )
        {
            // load the control block
            __m128i controlBlock       = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor ) );

            // split the control block into high and low nibbles
            __m128i controlHi          = _mm_and_si128( _mm_srli_epi32( controlBlock, CONTROL_BITS ), nibbleMask );
            __m128i controlLo          = _mm_and_si128( controlBlock, nibbleMask );

            // Note, the carries are set when the nibble is at its highest value, 15, meaning the operation after will
            // be an extension of the current match operation.

            // Work out the carry for the low nibbles (which will be used with the high controls to put them into 
            // match without offset read mode).
            __m128i carryLo            = _mm_cmpeq_epi8( controlLo, nibbleMask );

            // The carry for the high nibbles is used with the low controls, but needs one byte from the previous iteration. We save
            // the calculated carry to use that byte next iteration.
            __m128i carryHi            = _mm_cmpeq_epi8( controlHi, nibbleMask );
            __m128i shiftedCarryHi     = _mm_alignr_epi8( carryHi, previousCarryHi, 15 ); 

            previousCarryHi = carryHi;

            // We make the implicit assumption that the maximum number of literals to controls here is twice the offset size (4 vs 2),
            // we are doing this here to save keeping the value around (spilling or fetching it each time)
            __m128i literalsPerControl = _mm_add_epi8( offsetSize, offsetSize );

            // Here we are testing if the runs will be literals or matches. Note that if the carries are set from the previous operation
            // this will essentially be ignored later on.
            __m128i isLiteralHi        = _mm_cmplt_epi8( controlHi, literalsPerControl );
            __m128i isLiteralLo        = _mm_cmplt_epi8( controlLo, literalsPerControl );

            // Here we're calculating the number of bytes that will be output, we are actually subtracting negative one from the control 
            // (handy trick where comparison result masks are negative one) if carry is not set and it is a literal.
            __m128i bytesOutLo         = _mm_sub_epi8( controlLo, _mm_andnot_si128( shiftedCarryHi, isLiteralLo ) );
            __m128i bytesOutHi         = _mm_sub_epi8( controlHi, _mm_andnot_si128( carryLo, isLiteralHi ) ); 

            // Calculate the number of bytes to read per control.
            // In the case the carry is set, no bytes. Otherwise, the offset size (2 bytes) for matches or the number of output bytes for literals.
            __m128i streamBytesReadLo  = _mm_andnot_si128( shiftedCarryHi, _mm_blendv_epi8( offsetSize, bytesOutLo, isLiteralLo ) );
            __m128i streamBytesReadHi  = _mm_andnot_si128( carryLo, _mm_blendv_epi8( offsetSize, bytesOutHi, isLiteralHi ) );

            // I want 128 set bits please.
            __m128i allSet             = _mm_cmpeq_epi8( shiftedCarryHi, shiftedCarryHi ); 

            // Masks to read the offset (or keep the previous one) - set in the case that this is not a literal and the carry is not set
            __m128i readOffsetLo       = _mm_xor_si128( _mm_or_si128( isLiteralLo, shiftedCarryHi ), allSet );
            __m128i readOffsetHi       = _mm_xor_si128( _mm_or_si128( isLiteralHi, carryLo ), allSet );

            // Masks whether we are reading literals - set if the carry is not set and these are literals.
            __m128i fromLiteralLo      = _mm_andnot_si128( shiftedCarryHi, isLiteralLo );
            __m128i fromLiteralHi      = _mm_andnot_si128( carryLo, isLiteralHi );

            // Advance the input past the control block
            inputCursor += CONTROL_BLOCK_SIZE;

            {
                // Pull out the bottom halves off the SSE registers from before - we want these
                // things in GPRs for the more linear logic.
                uint64_t bytesOutHalfLo        = static_cast<uint64_t>( _mm_cvtsi128_si64( bytesOutLo ) );
                uint64_t bytesOutHalfHi        = static_cast<uint64_t>( _mm_cvtsi128_si64( bytesOutHi ) );

                uint64_t streamBytesReadHalfLo = static_cast<uint64_t>( _mm_cvtsi128_si64( streamBytesReadLo ) );
                uint64_t streamBytesReadHalfHi = static_cast<uint64_t>( _mm_cvtsi128_si64( streamBytesReadHi ) );

                uint64_t readOffsetHalfLo      = static_cast<uint64_t>( _mm_cvtsi128_si64( readOffsetLo ) );
                uint64_t readOffsetHalfHi      = static_cast<uint64_t>( _mm_cvtsi128_si64( readOffsetHi ) );

                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );
                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );

                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );
                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );

                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );
                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );

                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );
                DECODE_STEP_HALF_LO( true, false );
                DECODE_STEP_HALF_HI( true, false );
            }

            {
                // Now the top halves.
                uint64_t bytesOutHalfLo        = static_cast<uint64_t>( _mm_extract_epi64( bytesOutLo, 1 ) );
                uint64_t bytesOutHalfHi        = static_cast<uint64_t>( _mm_extract_epi64( bytesOutHi, 1 ) );

                uint64_t streamBytesReadHalfLo = static_cast<uint64_t>( _mm_extract_epi64( streamBytesReadLo, 1 ) );
                uint64_t streamBytesReadHalfHi = static_cast<uint64_t>( _mm_extract_epi64( streamBytesReadHi, 1 ) );

                uint64_t readOffsetHalfLo      = static_cast<uint64_t>( _mm_extract_epi64( readOffsetLo, 1 ) );
                uint64_t readOffsetHalfHi      = static_cast<uint64_t>( _mm_extract_epi64( readOffsetHi, 1 ) );

                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );
                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );

                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );
                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );

                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );
                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );

                DECODE_STEP_LO( true, false );
                DECODE_STEP_HI( true, false );
                DECODE_STEP_END_LO( true, false );
                DECODE_STEP_END_HI( true, false );
            }
        }

        // Decoding loop with no buffer checks, but will end at a safe distance from the end of the buffers.
        // Note, when we get here we have already reached the point in the output buffer which is *past* where we can underflow
        // due to a bad match offset.
        while ( outputCursor < outputSafeEnd && inputCursor < inputSafeEnd )
        {
            // This code is the same as the loop above, see comments there
            __m128i controlBlock       = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor ) );
            __m128i controlHi          = _mm_and_si128( _mm_srli_epi32( controlBlock, CONTROL_BITS ), nibbleMask );
            __m128i controlLo          = _mm_and_si128( controlBlock, nibbleMask );

            __m128i carryLo            = _mm_cmpeq_epi8( controlLo, nibbleMask );
            __m128i carryHi            = _mm_cmpeq_epi8( controlHi, nibbleMask );
            __m128i shiftedCarryHi     = _mm_alignr_epi8( carryHi, previousCarryHi, 15 ); // where we take the carry from the previous hi values

            previousCarryHi = carryHi;

            __m128i literalsPerControl = _mm_add_epi8( offsetSize, offsetSize );
            __m128i isLiteralHi        = _mm_cmplt_epi8( controlHi, literalsPerControl );
            __m128i isLiteralLo        = _mm_cmplt_epi8( controlLo, literalsPerControl );

            __m128i bytesOutLo         = _mm_sub_epi8( controlLo, _mm_andnot_si128( shiftedCarryHi, isLiteralLo ) );
            __m128i bytesOutHi         = _mm_sub_epi8( controlHi, _mm_andnot_si128( carryLo, isLiteralHi ) ); 

            __m128i streamBytesReadLo  = _mm_andnot_si128( shiftedCarryHi, _mm_blendv_epi8( offsetSize, bytesOutLo, isLiteralLo ) );
            __m128i streamBytesReadHi  = _mm_andnot_si128( carryLo, _mm_blendv_epi8( offsetSize, bytesOutHi, isLiteralHi ) );

            __m128i neg1               = _mm_cmpeq_epi8( shiftedCarryHi, shiftedCarryHi ); 

            __m128i readOffsetLo       = _mm_xor_si128( _mm_or_si128( isLiteralLo, shiftedCarryHi ), neg1 );
            __m128i readOffsetHi       = _mm_xor_si128( _mm_or_si128( isLiteralHi, carryLo ), neg1 );

            __m128i fromLiteralLo      = _mm_andnot_si128( shiftedCarryHi, isLiteralLo );
            __m128i fromLiteralHi      = _mm_andnot_si128( carryLo, isLiteralHi );

            inputCursor += CONTROL_BLOCK_SIZE;

            {
                uint64_t bytesOutHalfLo        = static_cast<uint64_t>( _mm_cvtsi128_si64( bytesOutLo ) );
                uint64_t bytesOutHalfHi        = static_cast<uint64_t>( _mm_cvtsi128_si64( bytesOutHi ) );

                uint64_t streamBytesReadHalfLo = static_cast<uint64_t>( _mm_cvtsi128_si64( streamBytesReadLo ) );
                uint64_t streamBytesReadHalfHi = static_cast<uint64_t>( _mm_cvtsi128_si64( streamBytesReadHi ) );

                uint64_t readOffsetHalfLo      = static_cast<uint64_t>( _mm_cvtsi128_si64( readOffsetLo ) );
                uint64_t readOffsetHalfHi      = static_cast<uint64_t>( _mm_cvtsi128_si64( readOffsetHi ) );

                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );
                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );

                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );
                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );

                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );
                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );

                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );
                DECODE_STEP_HALF_LO( false, false );
                DECODE_STEP_HALF_HI( false, false );
            }

            {
                uint64_t bytesOutHalfLo        = static_cast<uint64_t>( _mm_extract_epi64( bytesOutLo, 1 ) );
                uint64_t bytesOutHalfHi        = static_cast<uint64_t>( _mm_extract_epi64( bytesOutHi, 1 ) );

                uint64_t streamBytesReadHalfLo = static_cast<uint64_t>( _mm_extract_epi64( streamBytesReadLo, 1 ) );
                uint64_t streamBytesReadHalfHi = static_cast<uint64_t>( _mm_extract_epi64( streamBytesReadHi, 1 ) );

                uint64_t readOffsetHalfLo      = static_cast<uint64_t>( _mm_extract_epi64( readOffsetLo, 1 ) );
                uint64_t readOffsetHalfHi      = static_cast<uint64_t>( _mm_extract_epi64( readOffsetHi, 1 ) );

                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );
                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );

                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );
                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );

                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );
                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );

                DECODE_STEP_LO( false, false );
                DECODE_STEP_HI( false, false );
                DECODE_STEP_END_LO( false, false );
                DECODE_STEP_END_HI( false, false );
            }
        }
    }

    // Decoding loop with all buffer checks.
    {
        const  uint8_t* inputEarlyEnd;
        uint8_t*        outputEarlyEnd;
        inputEarlyEnd  = ( input + inputLength ) - END_PADDING_LITERALS;
        outputEarlyEnd = ( output + outputLength ) - END_PADDING_LITERALS;

        while ( outputCursor < outputEarlyEnd && inputCursor < inputEarlyEnd )
        {
            __m128i controlBlock       = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor ) );
            __m128i controlHi          = _mm_and_si128( _mm_srli_epi32( controlBlock, CONTROL_BITS ), nibbleMask );
            __m128i controlLo          = _mm_and_si128( controlBlock, nibbleMask );

            __m128i carryLo            = _mm_cmpeq_epi8( controlLo, nibbleMask );
            __m128i carryHi            = _mm_cmpeq_epi8( controlHi, nibbleMask );
            __m128i shiftedCarryHi     = _mm_alignr_epi8( carryHi, previousCarryHi, 15 ); 

            previousCarryHi = carryHi;

            __m128i literalsPerControl = _mm_add_epi8( offsetSize, offsetSize );
            __m128i isLiteralHi        = _mm_cmplt_epi8( controlHi, literalsPerControl );
            __m128i isLiteralLo        = _mm_cmplt_epi8( controlLo, literalsPerControl );

            __m128i bytesOutLo         = _mm_sub_epi8( controlLo, _mm_andnot_si128( shiftedCarryHi, isLiteralLo ) );
            __m128i bytesOutHi         = _mm_sub_epi8( controlHi, _mm_andnot_si128( carryLo, isLiteralHi ) );

            __m128i streamBytesReadLo  = _mm_andnot_si128( shiftedCarryHi, _mm_blendv_epi8( offsetSize, bytesOutLo, isLiteralLo ) );
            __m128i streamBytesReadHi  = _mm_andnot_si128( carryLo, _mm_blendv_epi8( offsetSize, bytesOutHi, isLiteralHi ) );

            __m128i neg1               = _mm_cmpeq_epi8( shiftedCarryHi, shiftedCarryHi ); 

            __m128i readOffsetLo       = _mm_xor_si128( _mm_or_si128( isLiteralLo, shiftedCarryHi ), neg1 );
            __m128i readOffsetHi       = _mm_xor_si128( _mm_or_si128( isLiteralHi, carryLo ), neg1 );

            __m128i fromLiteralLo      = _mm_andnot_si128( shiftedCarryHi, isLiteralLo );
            __m128i fromLiteralHi      = _mm_andnot_si128( carryLo, isLiteralHi );

            inputCursor += CONTROL_BLOCK_SIZE;

            if ( inputCursor > inputEarlyEnd )
                goto BUFFER_END;

            {
                uint64_t bytesOutHalfLo        = static_cast<uint64_t>( _mm_cvtsi128_si64( bytesOutLo ) );
                uint64_t bytesOutHalfHi        = static_cast<uint64_t>( _mm_cvtsi128_si64( bytesOutHi ) );

                uint64_t streamBytesReadHalfLo = static_cast<uint64_t>( _mm_cvtsi128_si64( streamBytesReadLo ) );
                uint64_t streamBytesReadHalfHi = static_cast<uint64_t>( _mm_cvtsi128_si64( streamBytesReadHi ) );

                uint64_t readOffsetHalfLo      = static_cast<uint64_t>( _mm_cvtsi128_si64( readOffsetLo ) );
                uint64_t readOffsetHalfHi      = static_cast<uint64_t>( _mm_cvtsi128_si64( readOffsetHi ) );

                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );
                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );

                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );
                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );

                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );
                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );

                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );
                DECODE_STEP_HALF_LO( true, true );
                DECODE_STEP_HALF_HI( true, true );
            }

            {
                // Now the top halves.
                uint64_t bytesOutHalfLo        = static_cast<uint64_t>( _mm_extract_epi64( bytesOutLo, 1 ) );
                uint64_t bytesOutHalfHi        = static_cast<uint64_t>( _mm_extract_epi64( bytesOutHi, 1 ) );

                uint64_t streamBytesReadHalfLo = static_cast<uint64_t>( _mm_extract_epi64( streamBytesReadLo, 1 ) );
                uint64_t streamBytesReadHalfHi = static_cast<uint64_t>( _mm_extract_epi64( streamBytesReadHi, 1 ) );

                uint64_t readOffsetHalfLo      = static_cast<uint64_t>( _mm_extract_epi64( readOffsetLo, 1 ) );
                uint64_t readOffsetHalfHi      = static_cast<uint64_t>( _mm_extract_epi64( readOffsetHi, 1 ) );

                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );
                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );

                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );
                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );

                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );
                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );

                DECODE_STEP_LO( true, true );
                DECODE_STEP_HI( true, true );
                DECODE_STEP_END_LO( true, true );
                DECODE_STEP_END_HI( true, true );
            }
        }

BUFFER_END:

        // When we get here, we have either advanced the right amount on both cursors
        // or something bad happened, so leave it as is, so we can tell where
        // the error happened. 
        if ( inputCursor == inputEarlyEnd && outputCursor == outputEarlyEnd )
        {
            size_t remainingLiterals = ( input + inputLength ) - inputCursor;

            // copy any trailing literals
            memcpy( outputCursor, inputCursor, remainingLiterals );

            outputCursor += remainingLiterals;
        }
    }

MATCH_UNDERFLOW:

    return outputCursor - output;
}
