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
		

#line 56 "control_verbs.cpp"
static const signed char _ControlVerbs_actions[] = {
			0, 1, 0, 1, 1, 1, 2, 1,
			3, 1, 4, 1, 5, 1, 6, 1,
			7, 1, 8, 1, 9, 0
		};
		
		static const short _ControlVerbs_key_offsets[] = {
			0, 7, 8, 10, 12, 14, 16, 18,
			20, 21, 23, 25, 27, 30, 32, 34,
			36, 38, 40, 42, 44, 46, 48, 50,
			52, 55, 57, 59, 61, 63, 66, 68,
			70, 72, 74, 76, 79, 82, 84, 86,
			88, 90, 92, 94, 96, 98, 100, 102,
			105, 107, 109, 111, 113, 115, 117, 119,
			121, 123, 125, 127, 129, 131, 133, 135,
			137, 139, 141, 143, 146, 148, 149, 151,
			155, 157, 159, 160, 161, 0
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
		
		static const signed char _ControlVerbs_single_lengths[] = {
			7, 1, 2, 2, 2, 2, 2, 2,
			1, 2, 2, 2, 3, 2, 2, 2,
			2, 2, 2, 2, 2, 2, 2, 2,
			3, 2, 2, 2, 2, 3, 2, 2,
			2, 2, 2, 1, 1, 2, 2, 2,
			2, 2, 2, 2, 2, 2, 2, 3,
			2, 2, 2, 2, 2, 2, 2, 2,
			2, 2, 2, 2, 2, 2, 2, 2,
			2, 2, 2, 3, 2, 1, 2, 4,
			2, 2, 1, 1, 1, 0
		};
		
		static const signed char _ControlVerbs_range_lengths[] = {
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 1, 1, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0
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
			225, 228, 231, 233, 235, 0
		};
		
		static const signed char _ControlVerbs_cond_targs[] = {
			75, 2, 9, 22, 24, 45, 67, 1,
			75, 1, 75, 3, 1, 75, 4, 1,
			75, 5, 1, 75, 6, 1, 75, 7,
			1, 75, 8, 1, 75, 1, 75, 10,
			1, 75, 11, 1, 75, 12, 1, 75,
			13, 16, 1, 75, 14, 1, 75, 15,
			1, 75, 5, 1, 75, 17, 1, 75,
			18, 1, 75, 19, 1, 75, 20, 1,
			75, 21, 1, 75, 8, 1, 75, 23,
			1, 75, 7, 1, 75, 8, 25, 1,
			75, 26, 1, 75, 27, 1, 75, 28,
			1, 75, 29, 1, 75, 30, 37, 1,
			75, 31, 1, 75, 32, 1, 75, 33,
			1, 75, 34, 1, 75, 35, 1, 75,
			36, 1, 75, 36, 1, 75, 38, 1,
			75, 39, 1, 75, 40, 1, 75, 41,
			1, 75, 42, 1, 75, 43, 1, 75,
			44, 1, 75, 34, 1, 75, 46, 1,
			75, 47, 1, 75, 48, 59, 1, 75,
			49, 1, 75, 50, 1, 75, 51, 1,
			75, 52, 1, 75, 53, 1, 75, 54,
			1, 75, 55, 1, 75, 56, 1, 75,
			57, 1, 75, 58, 1, 75, 8, 1,
			75, 60, 1, 75, 61, 1, 75, 62,
			1, 75, 63, 1, 75, 64, 1, 75,
			65, 1, 75, 66, 1, 75, 8, 1,
			75, 68, 70, 1, 75, 69, 1, 75,
			1, 75, 71, 1, 75, 72, 73, 74,
			1, 75, 8, 1, 75, 8, 1, 75,
			1, 76, 75, 0, 75, 75, 75, 75,
			75, 75, 75, 75, 75, 75, 75, 75,
			75, 75, 75, 75, 75, 75, 75, 75,
			75, 75, 75, 75, 75, 75, 75, 75,
			75, 75, 75, 75, 75, 75, 75, 75,
			75, 75, 75, 75, 75, 75, 75, 75,
			75, 75, 75, 75, 75, 75, 75, 75,
			75, 75, 75, 75, 75, 75, 75, 75,
			75, 75, 75, 75, 75, 75, 75, 75,
			75, 75, 75, 75, 75, 75, 75, 75,
			75, 75, 0
		};
		
		static const signed char _ControlVerbs_cond_actions[] = {
			19, 0, 0, 0, 0, 0, 0, 0,
			13, 0, 13, 0, 0, 13, 0, 0,
			11, 0, 0, 13, 0, 0, 13, 0,
			0, 13, 0, 0, 11, 0, 13, 0,
			0, 13, 0, 0, 13, 0, 0, 13,
			0, 0, 0, 13, 0, 0, 13, 0,
			0, 13, 0, 0, 13, 0, 0, 13,
			0, 0, 13, 0, 0, 13, 0, 0,
			13, 0, 0, 13, 0, 0, 13, 0,
			0, 11, 0, 0, 13, 0, 0, 0,
			13, 0, 0, 13, 0, 0, 13, 0,
			0, 13, 0, 0, 13, 0, 0, 0,
			13, 0, 0, 13, 0, 0, 13, 0,
			0, 13, 0, 0, 13, 0, 0, 13,
			0, 0, 11, 0, 0, 13, 0, 0,
			13, 0, 0, 13, 0, 0, 13, 0,
			0, 13, 0, 0, 13, 0, 0, 13,
			0, 0, 13, 0, 0, 13, 0, 0,
			13, 0, 0, 13, 0, 0, 0, 13,
			0, 0, 13, 0, 0, 13, 0, 0,
			13, 0, 0, 13, 0, 0, 13, 0,
			0, 13, 0, 0, 13, 0, 0, 13,
			0, 0, 13, 0, 0, 13, 0, 0,
			13, 0, 0, 13, 0, 0, 13, 0,
			0, 13, 0, 0, 13, 0, 0, 13,
			0, 0, 13, 0, 0, 13, 0, 0,
			13, 0, 0, 0, 13, 0, 0, 9,
			0, 13, 0, 0, 7, 0, 0, 0,
			0, 13, 0, 0, 13, 0, 0, 7,
			0, 5, 15, 0, 17, 19, 19, 19,
			19, 19, 19, 19, 19, 19, 19, 19,
			19, 19, 19, 19, 19, 19, 19, 19,
			19, 19, 19, 19, 19, 19, 19, 19,
			19, 19, 19, 19, 19, 19, 19, 19,
			19, 19, 19, 19, 19, 19, 19, 19,
			19, 19, 19, 19, 19, 19, 19, 19,
			19, 19, 19, 19, 19, 19, 19, 19,
			19, 19, 19, 19, 19, 19, 19, 19,
			19, 19, 19, 19, 19, 19, 19, 19,
			0, 17, 0
		};
		
		static const signed char _ControlVerbs_to_state_actions[] = {
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 1, 0, 0
		};
		
		static const signed char _ControlVerbs_from_state_actions[] = {
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 3, 0, 0
		};
		
		static const short _ControlVerbs_eof_trans[] = {
			238, 239, 240, 241, 242, 243, 244, 245,
			246, 247, 248, 249, 250, 251, 252, 253,
			254, 255, 256, 257, 258, 259, 260, 261,
			262, 263, 264, 265, 266, 267, 268, 269,
			270, 271, 272, 273, 274, 275, 276, 277,
			278, 279, 280, 281, 282, 283, 284, 285,
			286, 287, 288, 289, 290, 291, 292, 293,
			294, 295, 296, 297, 298, 299, 300, 301,
			302, 303, 304, 305, 306, 307, 308, 309,
			310, 311, 312, 313, 314, 0
		};
		
		static const int ControlVerbs_start = 75;
		static const int ControlVerbs_first_final = 75;
		static const int ControlVerbs_error = -1;
		
		static const int ControlVerbs_en_main = 75;
		

#line 269 "control_verbs.cpp"
	{
			cs = (int)ControlVerbs_start;
			ts = 0;
			te = 0;
		}
		
#line 105 "control_verbs.rl"

		
		try {

#line 278 "control_verbs.cpp"
	{
				int _klen;
				unsigned int _trans = 0;
				const char * _keys;
				const signed char * _acts;
				unsigned int _nacts;
				_resume: {}
				if ( p == pe && p != eof )
					goto _out;
				_acts = ( _ControlVerbs_actions + (_ControlVerbs_from_state_actions[cs]));
				_nacts = (unsigned int)(*( _acts));
				_acts += 1;
				while ( _nacts > 0 ) {
					switch ( (*( _acts)) ) {
						case 1:  {
								{
#line 1 "NONE"
								{ts = p;}}
							
#line 297 "control_verbs.cpp"

							break; 
						}
					}
					_nacts -= 1;
					_acts += 1;
				}
				
				if ( p == eof ) {
					if ( _ControlVerbs_eof_trans[cs] > 0 ) {
						_trans = (unsigned int)_ControlVerbs_eof_trans[cs] - 1;
					}
				}
				else {
					_keys = ( _ControlVerbs_trans_keys + (_ControlVerbs_key_offsets[cs]));
					_trans = (unsigned int)_ControlVerbs_index_offsets[cs];
					
					_klen = (int)_ControlVerbs_single_lengths[cs];
					if ( _klen > 0 ) {
						const char *_lower = _keys;
						const char *_upper = _keys + _klen - 1;
						const char *_mid;
						while ( 1 ) {
							if ( _upper < _lower ) {
								_keys += _klen;
								_trans += (unsigned int)_klen;
								break;
							}
							
							_mid = _lower + ((_upper-_lower) >> 1);
							if ( ( (*( p))) < (*( _mid)) )
								_upper = _mid - 1;
							else if ( ( (*( p))) > (*( _mid)) )
								_lower = _mid + 1;
							else {
								_trans += (unsigned int)(_mid - _keys);
								goto _match;
							}
						}
					}
					
					_klen = (int)_ControlVerbs_range_lengths[cs];
					if ( _klen > 0 ) {
						const char *_lower = _keys;
						const char *_upper = _keys + (_klen<<1) - 2;
						const char *_mid;
						while ( 1 ) {
							if ( _upper < _lower ) {
								_trans += (unsigned int)_klen;
								break;
							}
							
							_mid = _lower + (((_upper-_lower) >> 1) & ~1);
							if ( ( (*( p))) < (*( _mid)) )
								_upper = _mid - 2;
							else if ( ( (*( p))) > (*( _mid + 1)) )
								_lower = _mid + 2;
							else {
								_trans += (unsigned int)((_mid - _keys)>>1);
								break;
							}
						}
					}
					
					_match: {}
				}
				cs = (int)_ControlVerbs_cond_targs[_trans];
				
				if ( _ControlVerbs_cond_actions[_trans] != 0 ) {
					
					_acts = ( _ControlVerbs_actions + (_ControlVerbs_cond_actions[_trans]));
					_nacts = (unsigned int)(*( _acts));
					_acts += 1;
					while ( _nacts > 0 ) {
						switch ( (*( _acts)) )
						{
							case 2:  {
									{
#line 1 "NONE"
									{te = p+1;}}
								
#line 378 "control_verbs.cpp"

								break; 
							}
							case 3:  {
									{
#line 76 "control_verbs.rl"
									{te = p+1;{
#line 76 "control_verbs.rl"
											
											mode.utf8 = true;
										}
									}}
								
#line 391 "control_verbs.cpp"

								break; 
							}
							case 4:  {
									{
#line 80 "control_verbs.rl"
									{te = p+1;{
#line 80 "control_verbs.rl"
											
											mode.ucp = true;
										}
									}}
								
#line 404 "control_verbs.cpp"

								break; 
							}
							case 5:  {
									{
#line 84 "control_verbs.rl"
									{te = p+1;{
#line 84 "control_verbs.rl"
											
											ostringstream str;
											str << "Unsupported control verb " << string(ts, te - ts);
											throw LocatedParseError(str.str());
										}
									}}
								
#line 419 "control_verbs.cpp"

								break; 
							}
							case 6:  {
									{
#line 90 "control_verbs.rl"
									{te = p+1;{
#line 90 "control_verbs.rl"
											
											ostringstream str;
											str << "Unknown control verb " << string(ts, te - ts);
											throw LocatedParseError(str.str());
										}
									}}
								
#line 434 "control_verbs.cpp"

								break; 
							}
							case 7:  {
									{
#line 97 "control_verbs.rl"
									{te = p+1;{
#line 97 "control_verbs.rl"
											
											{p = p - 1; }
											{p += 1; goto _out; }
										}
									}}
								
#line 448 "control_verbs.cpp"

								break; 
							}
							case 8:  {
									{
#line 97 "control_verbs.rl"
									{te = p;p = p - 1;{
#line 97 "control_verbs.rl"
											
											{p = p - 1; }
											{p += 1; goto _out; }
										}
									}}
								
#line 462 "control_verbs.cpp"

								break; 
							}
							case 9:  {
									{
#line 97 "control_verbs.rl"
									{p = ((te))-1;
										{
#line 97 "control_verbs.rl"
											
											{p = p - 1; }
											{p += 1; goto _out; }
										}
									}}
								
#line 477 "control_verbs.cpp"

								break; 
							}
						}
						_nacts -= 1;
						_acts += 1;
					}
					
				}
				
				if ( p == eof ) {
					if ( cs >= 75 )
						goto _out;
				}
				else {
					_acts = ( _ControlVerbs_actions + (_ControlVerbs_to_state_actions[cs]));
					_nacts = (unsigned int)(*( _acts));
					_acts += 1;
					while ( _nacts > 0 ) {
						switch ( (*( _acts)) ) {
							case 0:  {
									{
#line 1 "NONE"
									{ts = 0;}}
								
#line 502 "control_verbs.cpp"

								break; 
							}
						}
						_nacts -= 1;
						_acts += 1;
					}
					
					p += 1;
					goto _resume;
				}
				_out: {}
			}
			
#line 108 "control_verbs.rl"

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
