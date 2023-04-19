
#line 1 "control_verbs.rl"
/*
 * Copyright (c) 2017, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Intel Corporation nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * \file
 * \brief Parser for control verbs that can occur at the beginning of a pattern.
 */

#include "parser/control_verbs.h"

#include "parser/Parser.h"
#include "parser/parse_error.h"

#include <cstring>
#include <sstream>

using namespace std;

namespace ue2 {

const char *read_control_verbs(const char *ptr, const char *end, size_t start,
                               ParseMode &mode) {
    const char *p = ptr;
    const char *pe = end;
    const char *eof = pe;
    const char *ts, *te;
    int cs;
    UNUSED int act;

    
#line 59 "control_verbs.cpp"
static const char _ControlVerbs_actions[] = {
	0, 1, 0, 1, 1, 1, 2, 1, 
	3, 1, 4, 1, 5, 1, 6, 1, 
	7, 1, 8, 1, 9
};

static const unsigned char _ControlVerbs_key_offsets[] = {
	0, 7, 8, 10, 12, 14, 16, 18, 
	20, 21, 23, 25, 27, 30, 32, 34, 
	36, 38, 40, 42, 44, 46, 48, 50, 
	52, 55, 57, 59, 61, 63, 66, 68, 
	70, 72, 74, 76, 79, 82, 84, 86, 
	88, 90, 92, 94, 96, 98, 100, 102, 
	105, 107, 109, 111, 113, 115, 117, 119, 
	121, 123, 125, 127, 129, 131, 133, 135, 
	137, 139, 141, 143, 146, 148, 149, 151, 
	155, 157, 159, 160, 161
};

static const char _ControlVerbs_trans_keys[] = {
	41, 65, 66, 67, 76, 78, 85, 41, 
	41, 78, 41, 89, 41, 67, 41, 82, 
	41, 76, 41, 70, 41, 41, 83, 41, 
	82, 41, 95, 41, 65, 85, 41, 78, 
	41, 89, 41, 67, 41, 78, 41, 73, 
	41, 67, 41, 79, 41, 68, 41, 69, 
	41, 82, 41, 76, 41, 70, 73, 41, 
	77, 41, 73, 41, 84, 41, 95, 41, 
	77, 82, 41, 65, 41, 84, 41, 67, 
	41, 72, 41, 61, 41, 48, 57, 41, 
	48, 57, 41, 69, 41, 67, 41, 85, 
	41, 82, 41, 83, 41, 73, 41, 79, 
	41, 78, 41, 79, 41, 95, 41, 65, 
	83, 41, 85, 41, 84, 41, 79, 41, 
	95, 41, 80, 41, 79, 41, 83, 41, 
	83, 41, 69, 41, 83, 41, 83, 41, 
	84, 41, 65, 41, 82, 41, 84, 41, 
	95, 41, 79, 41, 80, 41, 84, 41, 
	67, 84, 41, 80, 41, 41, 70, 41, 
	49, 51, 56, 41, 54, 41, 50, 41, 
	40, 42, 0
};

static const char _ControlVerbs_single_lengths[] = {
	7, 1, 2, 2, 2, 2, 2, 2, 
	1, 2, 2, 2, 3, 2, 2, 2, 
	2, 2, 2, 2, 2, 2, 2, 2, 
	3, 2, 2, 2, 2, 3, 2, 2, 
	2, 2, 2, 1, 1, 2, 2, 2, 
	2, 2, 2, 2, 2, 2, 2, 3, 
	2, 2, 2, 2, 2, 2, 2, 2, 
	2, 2, 2, 2, 2, 2, 2, 2, 
	2, 2, 2, 3, 2, 1, 2, 4, 
	2, 2, 1, 1, 1
};

static const char _ControlVerbs_range_lengths[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 1, 1, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0
};

static const short _ControlVerbs_index_offsets[] = {
	0, 8, 10, 13, 16, 19, 22, 25, 
	28, 30, 33, 36, 39, 43, 46, 49, 
	52, 55, 58, 61, 64, 67, 70, 73, 
	76, 80, 83, 86, 89, 92, 96, 99, 
	102, 105, 108, 111, 114, 117, 120, 123, 
	126, 129, 132, 135, 138, 141, 144, 147, 
	151, 154, 157, 160, 163, 166, 169, 172, 
	175, 178, 181, 184, 187, 190, 193, 196, 
	199, 202, 205, 208, 212, 215, 217, 220, 
	225, 228, 231, 233, 235
};

static const char _ControlVerbs_indicies[] = {
	0, 2, 3, 4, 5, 6, 7, 1, 
	8, 1, 8, 9, 1, 8, 10, 1, 
	11, 12, 1, 8, 13, 1, 8, 14, 
	1, 8, 15, 1, 11, 1, 8, 16, 
	1, 8, 17, 1, 8, 18, 1, 8, 
	19, 20, 1, 8, 21, 1, 8, 22, 
	1, 8, 12, 1, 8, 23, 1, 8, 
	24, 1, 8, 25, 1, 8, 26, 1, 
	8, 27, 1, 8, 15, 1, 8, 28, 
	1, 11, 14, 1, 8, 15, 29, 1, 
	8, 30, 1, 8, 31, 1, 8, 32, 
	1, 8, 33, 1, 8, 34, 35, 1, 
	8, 36, 1, 8, 37, 1, 8, 38, 
	1, 8, 39, 1, 8, 40, 1, 8, 
	41, 1, 11, 41, 1, 8, 42, 1, 
	8, 43, 1, 8, 44, 1, 8, 45, 
	1, 8, 46, 1, 8, 47, 1, 8, 
	48, 1, 8, 39, 1, 8, 49, 1, 
	8, 50, 1, 8, 51, 52, 1, 8, 
	53, 1, 8, 54, 1, 8, 55, 1, 
	8, 56, 1, 8, 57, 1, 8, 58, 
	1, 8, 59, 1, 8, 60, 1, 8, 
	61, 1, 8, 62, 1, 8, 15, 1, 
	8, 63, 1, 8, 64, 1, 8, 65, 
	1, 8, 66, 1, 8, 67, 1, 8, 
	68, 1, 8, 69, 1, 8, 15, 1, 
	8, 70, 71, 1, 8, 72, 1, 73, 
	1, 8, 74, 1, 75, 76, 77, 78, 
	1, 8, 15, 1, 8, 15, 1, 75, 
	1, 80, 79, 82, 81, 0
};

static const char _ControlVerbs_trans_targs[] = {
	75, 1, 2, 9, 22, 24, 45, 67, 
	75, 3, 4, 75, 5, 6, 7, 8, 
	10, 11, 12, 13, 16, 14, 15, 17, 
	18, 19, 20, 21, 23, 25, 26, 27, 
	28, 29, 30, 37, 31, 32, 33, 34, 
	35, 36, 38, 39, 40, 41, 42, 43, 
	44, 46, 47, 48, 59, 49, 50, 51, 
	52, 53, 54, 55, 56, 57, 58, 60, 
	61, 62, 63, 64, 65, 66, 68, 70, 
	69, 75, 71, 75, 72, 73, 74, 75, 
	76, 75, 0
};

static const char _ControlVerbs_trans_actions[] = {
	19, 0, 0, 0, 0, 0, 0, 0, 
	13, 0, 0, 11, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 9, 0, 7, 0, 0, 0, 15, 
	5, 17, 0
};

static const char _ControlVerbs_to_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 1, 0
};

static const char _ControlVerbs_from_state_actions[] = {
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 0, 0, 0, 0, 0, 
	0, 0, 0, 3, 0
};

static const short _ControlVerbs_eof_trans[] = {
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 1, 1, 1, 1, 1, 
	1, 1, 1, 0, 82
};

static const int ControlVerbs_start = 75;
static const int ControlVerbs_first_final = 75;
static const int ControlVerbs_error = -1;

static const int ControlVerbs_en_main = 75;


#line 249 "control_verbs.cpp"
	{
	cs = ControlVerbs_start;
	ts = 0;
	te = 0;
	act = 0;
	}

#line 105 "control_verbs.rl"


    try {
        
#line 262 "control_verbs.cpp"
	{
	int _klen;
	unsigned int _trans;
	const char *_acts;
	unsigned int _nacts;
	const char *_keys;

	if ( p == pe )
		goto _test_eof;
_resume:
	_acts = _ControlVerbs_actions + _ControlVerbs_from_state_actions[cs];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 ) {
		switch ( *_acts++ ) {
	case 1:
#line 1 "NONE"
	{ts = p;}
	break;
#line 281 "control_verbs.cpp"
		}
	}

	_keys = _ControlVerbs_trans_keys + _ControlVerbs_key_offsets[cs];
	_trans = _ControlVerbs_index_offsets[cs];

	_klen = _ControlVerbs_single_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + _klen - 1;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + ((_upper-_lower) >> 1);
			if ( (*p) < *_mid )
				_upper = _mid - 1;
			else if ( (*p) > *_mid )
				_lower = _mid + 1;
			else {
				_trans += (unsigned int)(_mid - _keys);
				goto _match;
			}
		}
		_keys += _klen;
		_trans += _klen;
	}

	_klen = _ControlVerbs_range_lengths[cs];
	if ( _klen > 0 ) {
		const char *_lower = _keys;
		const char *_mid;
		const char *_upper = _keys + (_klen<<1) - 2;
		while (1) {
			if ( _upper < _lower )
				break;

			_mid = _lower + (((_upper-_lower) >> 1) & ~1);
			if ( (*p) < _mid[0] )
				_upper = _mid - 2;
			else if ( (*p) > _mid[1] )
				_lower = _mid + 2;
			else {
				_trans += (unsigned int)((_mid - _keys)>>1);
				goto _match;
			}
		}
		_trans += _klen;
	}

_match:
	_trans = _ControlVerbs_indicies[_trans];
_eof_trans:
	cs = _ControlVerbs_trans_targs[_trans];

	if ( _ControlVerbs_trans_actions[_trans] == 0 )
		goto _again;

	_acts = _ControlVerbs_actions + _ControlVerbs_trans_actions[_trans];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 )
	{
		switch ( *_acts++ )
		{
	case 2:
#line 1 "NONE"
	{te = p+1;}
	break;
	case 3:
#line 76 "control_verbs.rl"
	{te = p+1;{
                mode.utf8 = true;
            }}
	break;
	case 4:
#line 80 "control_verbs.rl"
	{te = p+1;{
                mode.ucp = true;
            }}
	break;
	case 5:
#line 84 "control_verbs.rl"
	{te = p+1;{
                ostringstream str;
                str << "Unsupported control verb " << string(ts, te - ts);
                throw LocatedParseError(str.str());
            }}
	break;
	case 6:
#line 90 "control_verbs.rl"
	{te = p+1;{
                ostringstream str;
                str << "Unknown control verb " << string(ts, te - ts);
                throw LocatedParseError(str.str());
            }}
	break;
	case 7:
#line 97 "control_verbs.rl"
	{te = p+1;{
                p--;
                {p++; goto _out; }
            }}
	break;
	case 8:
#line 97 "control_verbs.rl"
	{te = p;p--;{
                p--;
                {p++; goto _out; }
            }}
	break;
	case 9:
#line 97 "control_verbs.rl"
	{{p = ((te))-1;}{
                p--;
                {p++; goto _out; }
            }}
	break;
#line 400 "control_verbs.cpp"
		}
	}

_again:
	_acts = _ControlVerbs_actions + _ControlVerbs_to_state_actions[cs];
	_nacts = (unsigned int) *_acts++;
	while ( _nacts-- > 0 ) {
		switch ( *_acts++ ) {
	case 0:
#line 1 "NONE"
	{ts = 0;}
	break;
#line 413 "control_verbs.cpp"
		}
	}

	if ( ++p != pe )
		goto _resume;
	_test_eof: {}
	if ( p == eof )
	{
	if ( _ControlVerbs_eof_trans[cs] > 0 ) {
		_trans = _ControlVerbs_eof_trans[cs] - 1;
		goto _eof_trans;
	}
	}

	_out: {}
	}

#line 109 "control_verbs.rl"
    } catch (LocatedParseError &error) {
        if (ts >= ptr && ts <= pe) {
            error.locate(ts - ptr + start);
        } else {
            error.locate(0);
        }
        throw;
    }

    return p;
}

} // namespace ue2
