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

#include "lzsse2_platform.h"
#include "lzsse2.h"

#pragma warning ( disable : 4127 )

namespace
{
    // Constants - most of these should not be changed without corresponding code changes because it will break many things in unpredictable ways.
    const uint32_t WINDOW_BITS             = 16;
    const uint32_t MIN_MATCH_LENGTH        = 3; 
    const uint32_t LZ_WINDOW_SIZE          = 1 << WINDOW_BITS;
    const uint32_t LZ_WINDOW_MASK          = LZ_WINDOW_SIZE - 1;
    const uint32_t OPTIMAL_HASH_BITS       = 20;
    const uint32_t OPTIMAL_BUCKETS_COUNT   = 1 << OPTIMAL_HASH_BITS;
    const uint32_t OPTIMAL_HASH_MASK       = OPTIMAL_BUCKETS_COUNT - 1;
    const uint32_t MIN_COMPRESSION_SIZE    = 32;
    const uint32_t END_PADDING_LITERALS    = 16;
    const int32_t  NO_MATCH                = -1;
    const int32_t  EMPTY_NODE              = -1;
    const uint32_t MIN_LITERAL_COUNT       = 8;
    const uint32_t HASH_MULTIPLIER         = 4013;
    const uint32_t HASH_REMOVAL_MULTIPLIER = HASH_MULTIPLIER * HASH_MULTIPLIER;
    const uint32_t CONTROL_BITS            = 4;
    const uint32_t LITERAL_BITS            = 8;
    const uint32_t OFFSET_BITS             = 16;
    const uint32_t BASE_MATCH_BITS         = OFFSET_BITS + CONTROL_BITS;
    const uint32_t SINGLE_LITERAL_COST     = CONTROL_BITS + LITERAL_BITS;
    const uint32_t DOUBLE_LITERAL_COST     = SINGLE_LITERAL_COST + LITERAL_BITS;
    const uint32_t EXTENDED_MATCH_BOUND    = ( 1 << CONTROL_BITS ) - 1;
    const uint32_t CONTROL_BLOCK_SIZE      = sizeof( __m128i );
    const uint32_t CONTROLS_PER_BLOCK      = 32;
    const uint32_t LITERALS_PER_CONTROL    = 2;
    const uint32_t MAX_INPUT_PER_CONTROL   = 2;
    const size_t   OUTPUT_BUFFER_SAFE      = EXTENDED_MATCH_BOUND * CONTROLS_PER_BLOCK;
    const size_t   INPUT_BUFFER_SAFE       = MAX_INPUT_PER_CONTROL * CONTROLS_PER_BLOCK;
    const uint16_t INITIAL_OFFSET          = MIN_MATCH_LENGTH;
    const size_t   SKIP_MATCH_LENGTH       = 128;
    const uint32_t NO_SKIP_LEVEL           = 17;
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

struct LZSSE2_OptimalParseState
{
    // Note, we should really replace this with a BST, hash chaining works but is *slooooooooooooooow* for optimal parse.
    int32_t roots[ OPTIMAL_BUCKETS_COUNT ];

    TreeNode window[ LZ_WINDOW_SIZE ];

    Arrival* arrivals;

    size_t bufferSize;
};


LZSSE2_OptimalParseState* LZSSE2_MakeOptimalParseState( size_t bufferSize )
{
    if ( bufferSize > 0 && ( SIZE_MAX / sizeof( Arrival ) ) < bufferSize )
    {
        return nullptr;
    }

    LZSSE2_OptimalParseState* result = reinterpret_cast< LZSSE2_OptimalParseState* >( ::malloc( sizeof( LZSSE2_OptimalParseState ) ) );

    result->bufferSize = bufferSize;

    if ( result != nullptr )
    {
        result->arrivals = reinterpret_cast< Arrival* >( ::malloc( sizeof( Arrival ) * bufferSize ) );

        if ( result->arrivals == nullptr )
        {
            LZSSE2_FreeOptimalParseState( result );

            result = nullptr;
        }
    }

    return result;
}


void LZSSE2_FreeOptimalParseState( LZSSE2_OptimalParseState* toFree )
{
    ::free( toFree->arrivals );
        
    toFree->arrivals = nullptr;

    ::free( toFree );
}


inline uint32_t CalculateHash( const uint8_t* inputCursor )
{
    return ( uint32_t( inputCursor[ 0 ] ) * HASH_MULTIPLIER * HASH_MULTIPLIER + uint32_t( inputCursor[ 1 ] ) * HASH_MULTIPLIER + uint32_t( inputCursor[ 2 ] ) ) & OPTIMAL_HASH_MASK;
}


struct Match
{
    size_t length;
    int32_t position;
    uint16_t offset;
};


inline Match SearchAndUpdateFinder( LZSSE2_OptimalParseState& state, const uint8_t* input, const uint8_t* inputCursor, const uint8_t* inputEnd, uint32_t cutOff )
{
    Match result;

    int32_t position = static_cast<int32_t>( inputCursor - input );

    result.position = NO_MATCH;
    result.length   = MIN_MATCH_LENGTH;
    result.offset   = 0;

    size_t   lengthToEnd  = inputEnd - inputCursor;
    int32_t  lastPosition = position - ( LZ_WINDOW_SIZE - 1 );
    uint32_t hash         = CalculateHash( inputCursor );

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

        uint16_t       matchOffset = static_cast<uint16_t>( position - treeCursor );
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

        if ( truncatedMatchLength >= result.length )
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
            const uint8_t* key         = input + matchPosition;
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
                result.length   = matchLength;
                result.offset   = matchOffset;
                result.position = matchPosition;
            }
        }
    }

    return result;
}


size_t LZSSE2_CompressOptimalParse( LZSSE2_OptimalParseState* state, const void* inputChar, size_t inputLength, void* outputChar, size_t outputLength, unsigned int level )
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
        /*Match dummy = */ SearchAndUpdateFinder( *state, input, inputCursor, inputEnd - END_PADDING_LITERALS, cutOff );

        ++inputCursor;
    }

    arrival->cost   = LITERAL_BITS * MIN_MATCH_LENGTH;
    arrival->from   = -1;
    arrival->offset = 0;
    
    // loop through each character and project forward the matches at that character to calculate the cheapest
    // path of arrival for each individual character.
    for ( const uint8_t* earlyEnd = inputEnd - END_PADDING_LITERALS; inputCursor < earlyEnd; ++inputCursor, ++arrival )
    {
        uint32_t  lengthToEnd     = static_cast< uint32_t >( earlyEnd - inputCursor );       
        int32_t   currentPosition = static_cast< int32_t >( inputCursor - input );
        Arrival*  literalFirst    = arrival + 1;
        Arrival*  literalSecond   = arrival + 2;
        size_t    arrivalCost     = arrival->cost;
        
        // NOTE - we currently assume only 2 literals filled in here, because the minimum match length is 3.
        // If we wanted to go with a higher minimum match length, we would need to fill in more literals before hand.
        // Also, because there is a maximum of 2 literals per control block assumed.

        // project forward the cost of a single literal
        if ( literalFirst > arrivalWatermark || literalFirst->cost > ( arrival->cost + SINGLE_LITERAL_COST ) )
        {
            literalFirst->cost   = arrival->cost + SINGLE_LITERAL_COST;
            literalFirst->from   = currentPosition;
            literalFirst->offset = 0;

            arrivalWatermark = literalFirst > arrivalWatermark ? literalFirst : arrivalWatermark;
        }

        // project forward the cost of two literals
        if ( lengthToEnd > 1 )
        {
            if ( literalSecond > arrivalWatermark || literalFirst->cost > ( arrival->cost + DOUBLE_LITERAL_COST ) )
            {
                literalSecond->cost   = arrival->cost + DOUBLE_LITERAL_COST;
                literalSecond->from   = currentPosition;
                literalSecond->offset = 0;

                arrivalWatermark = literalSecond > arrivalWatermark ? literalSecond : arrivalWatermark;
            }
        }
        else
        {
            continue;
        }

        Match match = SearchAndUpdateFinder( *state, input, inputCursor, earlyEnd, cutOff );
        
        if ( match.position != NO_MATCH )
        {
            for ( size_t matchedLength = MIN_MATCH_LENGTH, end = match.length + 1; matchedLength < end; ++matchedLength )
            {
                Arrival* matchArrival = arrival + matchedLength;
                size_t   matchCost    = arrivalCost + BASE_MATCH_BITS;

                if ( matchedLength > EXTENDED_MATCH_BOUND )
                {
                    matchCost += ( ( matchedLength - 1 ) / EXTENDED_MATCH_BOUND ) * CONTROL_BITS;
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
        previousPathNode     = state->arrivals + ( pathNode->from - MIN_MATCH_LENGTH );

        previousPathNode->to = static_cast<int32_t>( ( pathNode - state->arrivals ) + MIN_MATCH_LENGTH );
    }

    uint8_t* outputCursor = output;

    memcpy( outputCursor, input, MIN_MATCH_LENGTH );

    outputCursor += MIN_MATCH_LENGTH;

    uint8_t* currentControlBlock = outputCursor;
    uint32_t currentControlCount = 0;
    uint32_t totalControlCount   = 0;

    outputCursor += CONTROL_BLOCK_SIZE;

    Arrival* nextPathNode;

    size_t   totalPathLength  = MIN_MATCH_LENGTH;
    uint16_t previousOffset   = INITIAL_OFFSET;

    bool     lastControlIsNop = false;

    // Now walk forwards again and actually write out the data.
    for ( const Arrival* pathNode = state->arrivals; pathNode < arrivalWatermark; pathNode = nextPathNode )
    {
        int32_t currentPosition = static_cast< int32_t >( ( pathNode - state->arrivals ) + MIN_MATCH_LENGTH );

        nextPathNode = state->arrivals + ( pathNode->to - MIN_MATCH_LENGTH );

        size_t pathDistance = nextPathNode - pathNode;

        totalPathLength += pathDistance;

        lastControlIsNop = false;

        if ( pathDistance < MIN_MATCH_LENGTH )
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
            size_t toEncode = pathDistance - 1; //note, we always subtract one here, because the first control block of the match encodes this way

                                                // make sure the control block for the first part of the match has been allocated
                                                // note, this is idempontent if we have not actually incremented the control count and we try this again.
            if ( currentControlCount == CONTROLS_PER_BLOCK )
            {
                currentControlBlock  = outputCursor;
                outputCursor        += CONTROL_BLOCK_SIZE;
                currentControlCount  = 0;
            }

            // output the offset (after control block containing the first control nibble for this match).
            *reinterpret_cast< uint16_t* >( outputCursor ) = nextPathNode->offset ^ previousOffset;

            previousOffset = nextPathNode->offset;

            outputCursor += sizeof( uint16_t );

            for ( ;; )
            {
                if ( currentControlCount == CONTROLS_PER_BLOCK )
                {
                    currentControlBlock  = outputCursor;
                    outputCursor        += CONTROL_BLOCK_SIZE;
                    currentControlCount  = 0;
                }

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

                    ++totalControlCount;
                    ++currentControlCount;

                }
                else
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

                    ++totalControlCount;
                    ++currentControlCount;

                    break;
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


size_t LZSSE2_Decompress( const void* inputChar, size_t inputLength, void* outputChar, size_t outputLength )
{
    const uint8_t* input  = reinterpret_cast< const uint8_t* >( inputChar );
    uint8_t*       output = reinterpret_cast< uint8_t* >( outputChar );

    // Length it not work compressing, just copy initial values
    if ( outputLength == inputLength )
    {
        memcpy( output, input, outputLength );

        return inputLength;
    }

    const uint8_t* inputCursor  = input;
    uint8_t*       outputCursor = output;

    // The offset starts off as the minimum match length. We actually need it at least two
    // characters back because we need them to be set to xor out the literals from the match data.
    size_t  offset          = INITIAL_OFFSET;
    __m128i previousCarryHi = _mm_setzero_si128();

    *( outputCursor++ ) = *( inputCursor++ );
    *( outputCursor++ ) = *( inputCursor++ );
    *( outputCursor++ ) = *( inputCursor++ );

    // What these macros do:
    //     Decode a single literal run or match run for a single control nibble.
    // How they do it:
    //    - Read the *unaligned* input (in the case of LZSSE-F - twice, for LZSSE-O we read once) - one goes into an SSE register,
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
                                                                                                                                \
        uint64_t inputWord = *reinterpret_cast<const uint16_t*>( inputCursor );                                                 \
        __m128i  literals  = _mm_cvtsi64_si128( inputWord );                                                                    \
                                                                                                                                \
        offset ^= static_cast<size_t>( static_cast<ptrdiff_t>( static_cast<int8_t>( readOffsetHalf##HILO ) ) ) & inputWord;     \
                                                                                                                                \
        readOffsetHalf##HILO >>= 8;                                                                                             \
                                                                                                                                \
        const uint8_t* matchPointer = outputCursor - offset;                                                                    \
                                                                                                                                \
        if ( CHECKMATCH && matchPointer < output )                                                                              \
            goto MATCH_UNDERFLOW;                                                                                               \
                                                                                                                                \
        __m128i fromLiteral = _mm_shuffle_epi8( fromLiteral##HILO, _mm_setzero_si128() );                                       \
        __m128i matchData   = _mm_loadu_si128( reinterpret_cast<const __m128i*>( matchPointer ) );                              \
                                                                                                                                \
        literals = _mm_and_si128( fromLiteral, literals );                                                                      \
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
        size_t  inputWord   = *reinterpret_cast< const uint16_t* >( inputCursor );                                              \
        __m128i literals    = _mm_cvtsi64_si128( inputWord );                                                                   \
                                                                                                                                \
        offset ^= static_cast< size_t >( static_cast< ptrdiff_t >( static_cast<int8_t>( readOffsetHalf##HILO ) ) ) & inputWord; \
                                                                                                                                \
        const uint8_t* matchPointer = outputCursor - offset;                                                                    \
                                                                                                                                \
        if ( CHECKMATCH && matchPointer < output )                                                                              \
            goto MATCH_UNDERFLOW;                                                                                               \
                                                                                                                                \
        __m128i fromLiteral = _mm_shuffle_epi8( fromLiteral##HILO, _mm_setzero_si128() );                                       \
        __m128i matchData   = _mm_loadu_si128( reinterpret_cast<const __m128i*>( matchPointer ) );                              \
                                                                                                                                \
        literals = _mm_and_si128( fromLiteral, literals );                                                                      \
                                                                                                                                \
        fromLiteral##HILO = _mm_srli_si128( fromLiteral##HILO, 1 );                                                             \
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
        size_t  inputWord = *reinterpret_cast< const uint16_t* >( inputCursor );                                                \
        __m128i literals  = _mm_cvtsi64_si128( inputWord );                                                                     \
                                                                                                                                \
        offset ^= static_cast< size_t >( static_cast< ptrdiff_t >( static_cast<int8_t>( readOffsetHalf##HILO ) ) ) & inputWord; \
                                                                                                                                \
        const uint8_t* matchPointer = outputCursor - offset;                                                                    \
                                                                                                                                \
        if ( CHECKMATCH && matchPointer < output )                                                                              \
            goto MATCH_UNDERFLOW;                                                                                               \
                                                                                                                                \
        __m128i fromLiteral = _mm_shuffle_epi8( fromLiteral##HILO, _mm_setzero_si128() );                                       \
        __m128i matchData   = _mm_loadu_si128( reinterpret_cast<const __m128i*>( matchPointer ) );                              \
                                                                                                                                \
        literals = _mm_and_si128( fromLiteral, literals );                                                                      \
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

    __m128i nibbleMask             = _mm_set1_epi8( 0xF );
    __m128i literalsPerControl     = _mm_set1_epi8( LITERALS_PER_CONTROL );

    // Note, we use this block here because it allows the "fake" inputEarlyEnd/outputEarlyEnd not to cause register spills 
    // in the decompression loops. And yes, that did actually happen.
    {
#pragma warning ( push )
#pragma warning ( disable : 4101 )

        const  uint8_t* inputEarlyEnd; //= ( input + inputLength ) - END_PADDING_LITERALS;
        uint8_t*        outputEarlyEnd;// = ( output + outputLength ) - END_PADDING_LITERALS;

#pragma warning ( pop )

        // "Safe" ends to the buffer, before the input/output cursors hit these, we can loop without overflow checks.
        const  uint8_t* inputSafeEnd  = ( input + inputLength ) - INPUT_BUFFER_SAFE;
        uint8_t*        outputSafeEnd = ( output + outputLength ) - OUTPUT_BUFFER_SAFE;

        // Decoding loop with offset output buffer underflow test, but no buffer overflow tests, assumed to end at a safe distance 
        // from overflows
        while ( ( outputCursor - output ) < LZ_WINDOW_SIZE && outputCursor < outputSafeEnd && inputCursor < inputSafeEnd )
        {
            // load the control block
            __m128i controlBlock      = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor ) );

            // split the control block into high and low nibbles.
            __m128i controlHi         = _mm_and_si128( _mm_srli_epi32( controlBlock, CONTROL_BITS ), nibbleMask );
            __m128i controlLo         = _mm_and_si128( controlBlock, nibbleMask );
            
            // Here we are testing if the runs will be literals or matches. Note that if the carries are set from the previous operation
            // this will essentially be ignored later on.
            __m128i isLiteralHi       = _mm_cmplt_epi8( controlHi, literalsPerControl );
            __m128i isLiteralLo       = _mm_cmplt_epi8( controlLo, literalsPerControl );

            // Work out the carry for the low nibbles (which will be used with the high controls to put them into 
            // match without offset read mode).
            __m128i carryLo           = _mm_cmpeq_epi8( controlLo, nibbleMask );
            
            // The carry for the high nibbles is used with the low controls, but needs one byte from the previous iteration. We save
            // the calculated carry to use that byte next iteration.
            __m128i carryHi           = _mm_cmpeq_epi8( controlHi, nibbleMask );
            __m128i shiftedCarryHi    = _mm_alignr_epi8( carryHi, previousCarryHi, 15 );

            previousCarryHi = carryHi;

            // I want 128 set bits please.
            __m128i allSet            = _mm_cmpeq_epi8( shiftedCarryHi, shiftedCarryHi ); 

            // Calcualting the bytes to output to the stream. Basically, we are subtracting negative one from the control value if the 
            // carry is not set. This works because the masks produced by comparisons are the equivalent to negative one, which 
            // make this a conditional increment.
            __m128i bytesOutLo        = _mm_sub_epi8( controlLo, _mm_xor_si128( shiftedCarryHi, allSet ) );
            __m128i bytesOutHi        = _mm_sub_epi8( controlHi, _mm_xor_si128( carryLo, allSet ) );

            // Calculate the number of bytes to read per control.
            // In the case the carry is set, no bytes. Otherwise, the offset size (2 bytes) for matches or the number of output bytes for literals.
            __m128i streamBytesReadLo = _mm_andnot_si128( shiftedCarryHi, _mm_min_epi8( literalsPerControl, bytesOutLo ) );
            __m128i streamBytesReadHi = _mm_andnot_si128( carryLo, _mm_min_epi8( literalsPerControl, bytesOutHi ) );
            
            // Masks to read the offset (or keep the previous one) - set in the case that this is not a literal and the carry is not set
            __m128i readOffsetLo      = _mm_xor_si128( _mm_or_si128( isLiteralLo, shiftedCarryHi ), allSet );
            __m128i readOffsetHi      = _mm_xor_si128( _mm_or_si128( isLiteralHi, carryLo ), allSet );

            // Masks whether we are reading literals - set if the carry is not set and these are literals.
            __m128i fromLiteralLo     = _mm_andnot_si128( shiftedCarryHi, isLiteralLo );
            __m128i fromLiteralHi     = _mm_andnot_si128( carryLo, isLiteralHi );

            // Advance the input past the control block.
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
            __m128i controlBlock      = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor ) );
            __m128i controlHi         = _mm_and_si128( _mm_srli_epi32( controlBlock, CONTROL_BITS ), nibbleMask );
            __m128i controlLo         = _mm_and_si128( controlBlock, nibbleMask );

            __m128i isLiteralHi       = _mm_cmplt_epi8( controlHi, literalsPerControl );
            __m128i isLiteralLo       = _mm_cmplt_epi8( controlLo, literalsPerControl );
            __m128i carryLo           = _mm_cmpeq_epi8( controlLo, nibbleMask );
            __m128i carryHi           = _mm_cmpeq_epi8( controlHi, nibbleMask );
            __m128i shiftedCarryHi    = _mm_alignr_epi8( carryHi, previousCarryHi, 15 ); // where we take the carry from the previous hi values

            previousCarryHi = carryHi;

            __m128i neg1              = _mm_cmpeq_epi8( shiftedCarryHi, shiftedCarryHi );

            __m128i bytesOutLo        = _mm_sub_epi8( controlLo, _mm_xor_si128( shiftedCarryHi, neg1 ) );
            __m128i bytesOutHi        = _mm_sub_epi8( controlHi, _mm_xor_si128( carryLo, neg1 ) );

            __m128i streamBytesReadLo = _mm_andnot_si128( shiftedCarryHi, _mm_min_epi8( literalsPerControl, bytesOutLo ) );
            __m128i streamBytesReadHi = _mm_andnot_si128( carryLo, _mm_min_epi8( literalsPerControl, bytesOutHi ) );

            __m128i readOffsetLo      = _mm_xor_si128( _mm_or_si128( isLiteralLo, shiftedCarryHi ), neg1 );
            __m128i readOffsetHi      = _mm_xor_si128( _mm_or_si128( isLiteralHi, carryLo ), neg1 );

            __m128i fromLiteralLo     = _mm_andnot_si128( shiftedCarryHi, isLiteralLo );
            __m128i fromLiteralHi     = _mm_andnot_si128( carryLo, isLiteralHi );

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
        const uint8_t* inputEarlyEnd;
        uint8_t*        outputEarlyEnd;
        inputEarlyEnd  = (( input + inputLength ) - END_PADDING_LITERALS);
        outputEarlyEnd = ( output + outputLength ) - END_PADDING_LITERALS;

        while ( outputCursor < outputEarlyEnd && inputCursor < inputEarlyEnd )
        {
            __m128i controlBlock      = _mm_loadu_si128( reinterpret_cast<const __m128i*>( inputCursor ) );
            __m128i controlHi         = _mm_and_si128( _mm_srli_epi32( controlBlock, CONTROL_BITS ), nibbleMask );
            __m128i controlLo         = _mm_and_si128( controlBlock, nibbleMask );

            __m128i isLiteralHi       = _mm_cmplt_epi8( controlHi, literalsPerControl );
            __m128i isLiteralLo       = _mm_cmplt_epi8( controlLo, literalsPerControl );
            __m128i carryLo           = _mm_cmpeq_epi8( controlLo, nibbleMask );
            __m128i carryHi           = _mm_cmpeq_epi8( controlHi, nibbleMask );
            __m128i shiftedCarryHi    = _mm_alignr_epi8( carryHi, previousCarryHi, 15 ); // where we take the carry from the previous hi values

            previousCarryHi = carryHi;

            __m128i neg1              = _mm_cmpeq_epi8( shiftedCarryHi, shiftedCarryHi ); 

            __m128i bytesOutLo        = _mm_sub_epi8( controlLo, _mm_xor_si128( shiftedCarryHi, neg1 ) );
            __m128i bytesOutHi        = _mm_sub_epi8( controlHi, _mm_xor_si128( carryLo, neg1 ) );

            __m128i streamBytesReadLo = _mm_andnot_si128( shiftedCarryHi, _mm_min_epi8( literalsPerControl, bytesOutLo ) );
            __m128i streamBytesReadHi = _mm_andnot_si128( carryLo, _mm_min_epi8( literalsPerControl, bytesOutHi ) );

            __m128i readOffsetLo      = _mm_xor_si128( _mm_or_si128( isLiteralLo, shiftedCarryHi ), neg1 );
            __m128i readOffsetHi      = _mm_xor_si128( _mm_or_si128( isLiteralHi, carryLo ), neg1 );

            __m128i fromLiteralLo     = _mm_andnot_si128( shiftedCarryHi, isLiteralLo );
            __m128i fromLiteralHi     = _mm_andnot_si128( carryLo, isLiteralHi );

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
