// Copyright (C) 2006 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


/**
 * @fileoverview
 * some functions for browser-side pretty printing of code contained in html.
 *
 * The lexer should work on a number of languages including C and friends,
 * Java, Python, Bash, SQL, HTML, XML, CSS, Javascript, and Makefiles.
 * It works passably on Ruby, PHP and Awk and a decent subset of Perl, but,
 * because of commenting conventions, doesn't work on Smalltalk, Lisp-like, or
 * CAML-like languages.
 *
 * If there's a language not mentioned here, then I don't know it, and don't
 * know whether it works.  If it has a C-like, Bash-like, or XML-like syntax
 * then it should work passably.
 *
 * Usage:
 * 1) include this source file in an html page via
 * <script type="text/javascript" src="/path/to/prettify.js"></script>
 * 2) define style rules.  See the example page for examples.
 * 3) mark the <pre> and <code> tags in your source with class=prettyprint.
 *    You can also use the (html deprecated) <xmp> tag, but the pretty printer
 *    needs to do more substantial DOM manipulations to support that, so some
 *    css styles may not be preserved.
 * That's it.  I wanted to keep the API as simple as possible, so there's no
 * need to specify which language the code is in.
 *
 * Change log:
 * cbeust, 2006/08/22
 *   Java annotations (start with "@") are now captured as literals ("lit")
 */

var PR_keywords = new Object();
/** initialize the keyword list for our target languages. */
(function () {
  var CPP_KEYWORDS = (
    "bool break case catch char class const const_cast continue default " +
    "delete deprecated dllexport dllimport do double dynamic_cast else enum " +
    "explicit extern false float for friend goto if inline int long mutable " +
    "naked namespace new noinline noreturn nothrow novtable operator private " +
    "property protected public register reinterpret_cast return selectany " +
    "short signed sizeof static static_cast struct switch template this " +
    "thread throw true try typedef typeid typename union unsigned using " +
    "declaration, using directive uuid virtual void volatile while typeof");
  var JAVA_KEYWORDS = (
    "abstract default goto package synchronized boolean do if private this " +
    "break double implements protected throw byte else import public throws " +
    "case enum instanceof return transient catch extends int short try char " +
    "final interface static void class finally long strictfp volatile const " +
    "float native super while continue for new switch");
  var PYTHON_KEYWORDS = (
    "and assert break class continue def del elif else except exec finally " +
    "for from global if import in is lambda not or pass print raise return " +
    "try while yield False True None");
  var JSCRIPT_KEYWORDS = (
    "abstract boolean break byte case catch char class const continue " +
    "debugger default delete do double else enum export extends false final " +
    "finally float for function goto if implements import in instanceof int " +
    "interface long native new null package private protected public return " +
    "short static super switch synchronized this throw throws transient " +
    "true try typeof var void volatile while with NaN Infinity");
  var PERL_KEYWORDS = (
    "foreach require sub unless until use elsif BEGIN END");
  var SH_KEYWORDS = (
    "if then do done else fi end");
  var RUBY_KEYWORDS = (
      "if then elsif else end begin do rescue ensure while for class module " +
      "def yield raise until unless and or not when case super undef break " +
      "next redo retry in return alias defined");
  var KEYWORDS = [CPP_KEYWORDS, JAVA_KEYWORDS, JSCRIPT_KEYWORDS, PERL_KEYWORDS,
                  PYTHON_KEYWORDS, RUBY_KEYWORDS, SH_KEYWORDS];
  for (var k = 0; k < KEYWORDS.length; k++) {
    var kw = KEYWORDS[k].split(' ');
    for (var i = 0; i < kw.length; i++) {
      if (kw[i]) { PR_keywords[kw[i]] = true; }
    }
  }
}).call(this);

// token style names.  correspond to css classes
/** token style for a string literal */
var PR_STRING = 'str';
/** token style for a keyword */
var PR_KEYWORD = 'kwd';
/** token style for a comment */
var PR_COMMENT = 'com';
/** token style for a type */
var PR_TYPE = 'typ';
/** token style for a literal value.  e.g. 1, null, true. */
var PR_LITERAL = 'lit';
/** token style for a punctuation string. */
var PR_PUNCTUATION = 'pun';
/** token style for a punctuation string. */
var PR_PLAIN = 'pln';

/** token style for an sgml tag. */
var PR_TAG = 'tag';
/** token style for a markup declaration such as a DOCTYPE. */
var PR_DECLARATION = 'dec';
/** token style for embedded source. */
var PR_SOURCE = 'src';
/** token style for an sgml attribute name. */
var PR_ATTRIB_NAME = 'atn';
/** token style for an sgml attribute value. */
var PR_ATTRIB_VALUE = 'atv';

/** the position of the end of a token during.  A division of a string into
  * n tokens can be represented as a series n - 1 token ends, as long as
  * runs of whitespace warrant their own token.
  * @private
  */
function PR_TokenEnd(end, style) {
  if (undefined === style) { throw new Error('BAD'); }
  if ('number' != typeof(end)) { throw new Error('BAD'); }
  this.end = end;
  this.style = style;
}
PR_TokenEnd.prototype.toString = function () {
  return '[PR_TokenEnd ' + this.end +
    (this.style ? ':' + this.style : '') + ']';
};


/** a chunk of text with a style.  These are used to represent both the output
  * from the lexing functions as well as intermediate results.
  * @constructor
  * @param token the token text
  * @param style one of the token styles defined in designdoc-template, or null
  *   for a styleless token, such as an embedded html tag.
  * @private
  */
function PR_Token(token, style) {
  if (undefined === style) { throw new Error('BAD'); }
  this.token = token;
  this.style = style;
}

PR_Token.prototype.toString = function () {
  return '[PR_Token ' + this.token + (this.style ? ':' + this.style : '') + ']';
};


/** a helper class that decodes common html entities used to escape source and
  * markup punctuation characters in html.
  * @constructor
  * @private
  */
function PR_DecodeHelper() {
  this.next = 0;
  this.ch = '\0';
}

PR_DecodeHelper.prototype.decode = function (s, i) {
  var next = i + 1;
  var ch = s.charAt(i);
  if ('&' == ch) {
    var semi = s.indexOf(';', next);
    if (semi >= 0 && semi < next + 4) {
      var entityName = s.substring(next, semi).toLowerCase();
      next = semi + 1;
      if ('lt' == entityName) {
        ch = '<';
      } else if ('gt' == entityName) {
        ch = '>';
      } else if ('quot' == entityName) {
        ch = '"';
      } else if ('apos' == entityName) {
        ch = '\'';
      } else if ('amp' == entityName) {
        ch = '&';
      } else {
        next = i + 1;
      }
    }
  }
  this.next = next;
  this.ch = ch;
  return this.ch;
}


// some string utilities
function PR_isWordChar(ch) {
  return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
}

function PR_isIdentifierStart(ch) {
  return PR_isWordChar(ch) || ch == '_' || ch == '$' || ch == '@';
}

function PR_isIdentifierPart(ch) {
  return PR_isIdentifierStart(ch) || PR_isDigitChar(ch);
}

function PR_isSpaceChar(ch) {
  return "\t \r\n".indexOf(ch) >= 0;
}

function PR_isDigitChar(ch) {
  return ch >= '0' && ch <= '9';
}

function PR_trim(s) {
  var i = 0, j = s.length - 1;
  while (i <= j && PR_isSpaceChar(s.charAt(i))) { ++i; }
  while (j > i && PR_isSpaceChar(s.charAt(j))) { --j; }
  return s.substring(i, j + 1);
}

function PR_startsWith(s, prefix) {
  return s.length >= prefix.length && prefix == s.substring(0, prefix.length);
}

function PR_endsWith(s, suffix) {
  return s.length >= suffix.length &&
         suffix == s.substring(s.length - suffix.length, s.length);
}

/** true iff prefix matches the first prefix characters in chars[0:len].
  * @private
  */
function PR_prefixMatch(chars, len, prefix) {
  if (len < prefix.length) { return false; }
  for (var i = 0, n = prefix.length; i < n; ++i) {
    if (prefix.charAt(i) != chars[i]) { return false; }
  }
  return true;
}

/** like textToHtml but escapes double quotes to be attribute safe. */
function PR_attribToHtml(str) {
  return str.replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\"/g, '&quot;')
    .replace(/\xa0/, '&nbsp;');
}

/** escapest html special characters to html. */
function PR_textToHtml(str) {
  return str.replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/\xa0/g, '&nbsp;');
}

/** is the given node's innerHTML normally unescaped? */
function PR_isRawContent(node) {
  return 'XMP' == node.tagName;
}

var PR_innerHtmlWorks = null;
function PR_getInnerHtml(node) {
  // inner html is hopelessly broken in Safari 2.0.4 when the content is
  // an html description of well formed XML and the containing tag is a PRE
   // tag, so we detect that case and emulate innerHTML.
  if (null == PR_innerHtmlWorks) {
    var testNode = document.createElement('PRE');
    testNode.appendChild(
        document.createTextNode('<!DOCTYPE foo PUBLIC "foo bar">\n<foo />'));
    PR_innerHtmlWorks = !/</.test(testNode.innerHTML);
  }

  if (PR_innerHtmlWorks) {
    var content = node.innerHTML;
    // XMP tags contain unescaped entities so require special handling.
    if (PR_isRawContent(node)) {
       content = PR_textToHtml(content);
    }
    return content;
  }

  var out = [];
  for (var child = node.firstChild; child; child = child.nextSibling) {
    PR_normalizedHtml(child, out);
  }
  return out.join('');
}

/**
 * walks the DOM returning a properly escaped version of innerHTML.
 */
function PR_normalizedHtml(node, out) {
  switch (node.nodeType) {
    case 1:  // an element
      var name = node.tagName.toLowerCase();
      out.push('\074', name);
      for (var i = 0; i < node.attributes.length; ++i) {
        var attr = node.attributes[i];
        if (!attr.specified) { continue; }
        out.push(' ');
        PR_normalizedHtml(attr, out);
      }
      out.push('>');
      for (var child = node.firstChild; child; child = child.nextSibling) {
        PR_normalizedHtml(child, out);
      }
      if (node.firstChild || !/^(?:br|link|img)$/.test(name)) {
        out.push('<\/', name, '>');
      }
      break;
    case 2: // an attribute
      out.push(node.name.toLowerCase(), '="', PR_attribToHtml(node.value), '"');
      break;
    case 3: case 4: // text
      out.push(PR_textToHtml(node.nodeValue));
      break;
  }
}


/** split markup into chunks of html tags (style null) and
  * plain text (style {@link #PR_PLAIN}).
  *
  * @param s a String of html.
  * @return an Array of PR_Tokens of style PR_PLAIN and null.
  * @private
  */
function PR_chunkify(s) {
  var chunks = new Array();
  var state = 0;
  var start = 0;
  var pos = -1;
  for (var i = 0, n = s.length; i < n; ++i) {
    var ch = s.charAt(i);
    switch (state) {
      case 0:
        if ('<' == ch) { state = 1; }
        break;
      case 1:
        pos = i - 1;
        if ('/' == ch) { state = 2; }
        else if (PR_isWordChar(ch)) { state = 3; }
        else if ('<' == ch) { state = 1; }
        else { state = 0; }
        break;
      case 2:
        if (PR_isWordChar(ch)) { state = 3; }
        else if ('<' == ch) { state = 1; }
        else { state = 0; }
        break;
      case 3:
        if ('>' == ch) {
          if (pos > start) {
            chunks.push(new PR_Token(s.substring(start, pos), PR_PLAIN));
          }
          chunks.push(new PR_Token(s.substring(pos, i + 1), null));
          start = i + 1;
          pos = -1;
          state = 0;
        }
        break;
    }
  }
  if (s.length > start) {
    chunks.push(new PR_Token(s.substring(start, s.length), PR_PLAIN));
  }
  return chunks;
}

/** splits chunks around entities.
  * @private
  */
function PR_splitEntities(chunks) {
  var chunksOut = new Array();
  var state = 0;
  for (var ci = 0, nc = chunks.length; ci < nc; ++ci) {
    var chunk = chunks[ci];
    if (PR_PLAIN != chunk.style) {
      chunksOut.push(chunk);
      continue;
    }
    var s = chunk.token;
    var pos = 0;
    var start;
    for (var i = 0; i < s.length; ++i) {
      var ch = s.charAt(i);
      switch (state) {
        case 0:
          if ('&' == ch) { state = 1; }
          break;
        case 1:
          if ('#' == ch || PR_isWordChar(ch)) {
            start = i - 1;
            state = 2;
          } else {
            state = 0;
          }
          break;
        case 2:
          if (';' == ch) {
            if (start > pos) {
              chunksOut.push(
                  new PR_Token(s.substring(pos, start), chunk.style));
            }
            chunksOut.push(new PR_Token(s.substring(start, i + 1), null));
            pos = i + 1;
            state = 0;
          }
          break;
      }
    }
    if (s.length > pos) {
      chunksOut.push(pos ?
                     new PR_Token(s.substring(pos, s.length), chunk.style) :
                     chunk);
    }
  }
  return chunksOut;
}

/** walk the tokenEnds list and the chunk list in parallel to generate a list
  * of split tokens.
  * @private
  */
function PR_splitChunks(chunks, tokenEnds) {
  var tokens = new Array();  // the output

  var ci = 0;  // index into chunks
  // position of beginning of amount written so far in absolute space.
  var posAbs = 0;
  // position of amount written so far in chunk space
  var posChunk = 0;

  // current chunk
  var chunk = new PR_Token('', null);

  for (var ei = 0, ne = tokenEnds.length; ei < ne; ++ei) {
    var tokenEnd = tokenEnds[ei];
    var end = tokenEnd.end;

    var tokLen = end - posAbs;
    var remainingInChunk = chunk.token.length - posChunk;
    while (remainingInChunk <= tokLen) {
      if (remainingInChunk > 0) {
        tokens.push(
            new PR_Token(chunk.token.substring(posChunk, chunk.token.length),
                         null == chunk.style ? null : tokenEnd.style));
      }
      posAbs += remainingInChunk;
      posChunk = 0;
      if (ci < chunks.length) { chunk = chunks[ci++]; }

      tokLen = end - posAbs;
      remainingInChunk = chunk.token.length - posChunk;
    }

    if (tokLen) {
      tokens.push(
          new PR_Token(chunk.token.substring(posChunk, posChunk + tokLen),
                       tokenEnd.style));
      posAbs += tokLen;
      posChunk += tokLen;
    }
  }

  return tokens;
}

/** splits markup tokens into declarations, tags, and source chunks.
  * @private
  */
function PR_splitMarkup(chunks) {
  // A state machine to split out declarations, tags, etc.
  // This state machine deals with absolute space in the text, indexed by k,
  // and position in the current chunk, indexed by pos and tokenStart to
  // generate a list of the ends of tokens.
  // Absolute space is calculated by considering the chunks as appended into
  // one big string, as they were before being split.

  // Known failure cases
  // Server side scripting sections such as <?...?> in attributes.
  // i.e. <span class="<? foo ?>">
  // Handling this would require a stack, and we don't use PHP.

  // The output: a list of pairs of PR_TokenEnd instances
  var tokenEnds = new Array();

  var state = 0;  // FSM state variable
  var k = 0;  // position in absolute space of the start of the current chunk
  var tokenStart = -1;  // the start of the current token

  // Try to find a closing tag for any open <style> or <script> tags
  // We can't do this at a later stage because then the following case
  // would fail:
  // <script>document.writeln('<!--');</script>

  // We use tokenChars[:tokenCharsI] to accumulate the tag name so that we
  // can check whether to enter into a no scripting section when the tag ends.
  var tokenChars = new Array(12);
  var tokenCharsI = 0;
  // if non null, the tag prefix that we need to see to break out.
  var endScriptTag = null;
  var decodeHelper = new PR_DecodeHelper();

  for (var ci = 0, nc = chunks.length; ci < nc; ++ci) {
    var chunk = chunks[ci];
    if (PR_PLAIN != chunk.style) {
      k += chunk.token.length;
      continue;
    }

    var s = chunk.token;
    var pos = 0;  // the position past the last character processed so far in s

    for (var i = 0, n = s.length; i < n; /* i = next at bottom */) {
      decodeHelper.decode(s, i);
      var ch = decodeHelper.ch;
      var next = decodeHelper.next;

      var tokenStyle = null;
      switch (state) {
        case 0:
          if ('<' == ch) { state = 1; }
          break;
        case 1:
          tokenCharsI = 0;
          if ('/' == ch) {  // only consider close tags if we're in script/style
            state = 7;
          } else if (null == endScriptTag) {
            if ('!' == ch) {
              state = 2;
            } else if (PR_isWordChar(ch)) {
              state = 8;
            } else if ('?' == ch) {
              state = 9;
            } else if ('%' == ch) {
              state = 11;
            } else if ('<' != ch) {
              state = 0;
            }
          } else if ('<' != ch) {
            state = 0;
          }
          break;
        case 2:
          if ('-' == ch) {
            state = 4;
          } else if (PR_isWordChar(ch)) {
            state = 3;
          } else if ('<' == ch) {
            state = 1;
          } else {
            state = 0;
          }
          break;
        case 3:
          if ('>' == ch) {
            state = 0;
            tokenStyle = PR_DECLARATION;
          }
          break;
        case 4:
          if ('-' == ch) { state = 5; }
          break;
        case 5:
          if ('-' == ch) { state = 6; }
          break;
        case 6:
          if ('>' == ch) {
            state = 0;
            tokenStyle = PR_COMMENT;
          } else if ('-' == ch) {
            state = 6;
          } else {
            state = 4;
          }
          break;
        case 7:
          if (PR_isWordChar(ch)) {
            state = 8;
          } else if ('<' == ch) {
            state = 1;
          } else {
            state = 0;
          }
          break;
        case 8:
          if ('>' == ch) {
            state = 0;
            tokenStyle = PR_TAG;
          }
          break;
        case 9:
          if ('?' == ch) { state = 10; }
          break;
        case 10:
          if ('>' == ch) {
            state = 0;
            tokenStyle = PR_SOURCE;
          } else if ('?' != ch) {
            state = 9;
          }
          break;
        case 11:
          if ('%' == ch) { state = 12; }
          break;
        case 12:
          if ('>' == ch) {
            state = 0;
            tokenStyle = PR_SOURCE;
          } else if ('%' != ch) {
            state = 11;
          }
          break;
      }

      if (tokenCharsI < tokenChars.length) {
        tokenChars[tokenCharsI++] = ch.toLowerCase();
      }
      if (1 == state) { tokenStart = k + i; }
      i = next;
      if (tokenStyle != null) {
        if (null != tokenStyle) {
          if (endScriptTag) {
            if (PR_prefixMatch(tokenChars, tokenCharsI, endScriptTag)) {
              endScriptTag = null;
            }
          } else {
            if (PR_prefixMatch(tokenChars, tokenCharsI, 'script')) {
              endScriptTag = '/script';
            } else if (PR_prefixMatch(tokenChars, tokenCharsI, 'style')) {
              endScriptTag = '/style';
            } else if (PR_prefixMatch(tokenChars, tokenCharsI, 'xmp')) {
              endScriptTag = '/xmp';
            }
          }
          // disallow the tag if endScriptTag is set and this was not an open
          // tag.
          if (endScriptTag && tokenCharsI && '/' == tokenChars[0]) {
            tokenStyle = null;
          }
        }
        if (null != tokenStyle) {
          tokenEnds.push(new PR_TokenEnd(tokenStart, PR_PLAIN));
          tokenEnds.push(new PR_TokenEnd(k + next, tokenStyle));
        }
      }
    }
    k += chunk.token.length;
  }
  tokenEnds.push(new PR_TokenEnd(k, PR_PLAIN));

  return tokenEnds;
}

/** splits the given string into comment, string, and "other" tokens.
  * @return an array of PR_Tokens with style in
  *   (PR_STRING, PR_COMMENT, PR_PLAIN, null)
  *   The result array may contain spurious zero length tokens.  Ignore them.
  *
  * @private
  */
function PR_splitStringAndCommentTokens(chunks) {
  // a state machine to split out comments, strings, and other stuff
  var tokenEnds = new Array();  // positions of ends of tokens in absolute space
  var state = 0;  // FSM state variable
  var delim = -1;  // string delimiter
  var k = 0;  // absolute position of beginning of current chunk
  for (var ci = 0, nc = chunks.length; ci < nc; ++ci) {
    var chunk = chunks[ci];
    var s = chunk.token;
    if (PR_PLAIN == chunk.style) {
      for (var i = 0, n = s.length; i < n; ++i) {
        var ch = s.charAt(i);
        if (0 == state) {
          if (ch == '"' || ch == '\'' || ch == '`') {
            tokenEnds.push(new PR_TokenEnd(k + i, PR_PLAIN));
            state = 1;
            delim = ch;
          } else if (ch == '/') {
            state = 3;
          } else if (ch == '#') {
            tokenEnds.push(new PR_TokenEnd(k + i, PR_PLAIN));
            state = 4;
          }
        } else if (1 == state) {
          if (ch == delim) {
            state = 0;
            tokenEnds.push(new PR_TokenEnd(k + i + 1, PR_STRING));
          } else if (ch == '\\') {
            state = 2;
          }
        } else if (2 == state) {
          state = 1;
        } else if (3 == state) {
          if (ch == '/') {
            state = 4;
            tokenEnds.push(new PR_TokenEnd(k + i - 1, PR_PLAIN));
          } else if (ch == '*') {
            state = 5;
            tokenEnds.push(new PR_TokenEnd(k + i - 1, PR_PLAIN));
          } else {
            state = 0;
            // next loop will reenter state 0 without same value of i, so
            // ch will be reconsidered as start of new token.
            --i;
          }
        } else if (4 == state) {
          if (ch == '\r' || ch == '\n') {
            state = 0;
            tokenEnds.push(new PR_TokenEnd(k + i, PR_COMMENT));
          }
        } else if (5 == state) {
          if (ch == '*') {
            state = 6;
          }
        } else if (6 == state) {
          if (ch == '/') {
            state = 0;
            tokenEnds.push(new PR_TokenEnd(k + i + 1, PR_COMMENT));
          } else if (ch != '*') {
            state = 5;
          }
        }
      }
    }
    k += s.length;
  }
  var endTokenType;
  switch (state) {
    case 1: case 2:
      endTokenType = PR_STRING;
      break;
    case 4: case 5: case 6:
      endTokenType = PR_COMMENT;
      break;
    default:
      endTokenType = PR_PLAIN;
      break;
  }
  // handle unclosed token which can legally happen for line comments (state 4)
  tokenEnds.push(new PR_TokenEnd(k, endTokenType));  // a token ends at the end

  return PR_splitChunks(chunks, tokenEnds);
}

/** used by lexSource to split a non string, non comment token.
  * @private
  */
function PR_splitNonStringNonCommentToken(s, outlist) {
  var pos = 0;
  var state = 0;
  for (var i = 0; i <= s.length; i++) {
    var ch = s.charAt(i);
    // the next state.
    // if set to -1 then it will cause a reentry to state 0 without consuming
    // another character.
    var nstate = state;

    if (i == s.length) {
      // nstate will not be equal to state, so it will append the token
      nstate = -2;
    } else {
      switch (state) {
      case 0:  // whitespace state
        if (PR_isIdentifierStart(ch)) {
          nstate = 1;
        } else if (PR_isDigitChar(ch)) {
          nstate = 2;
        } else if (!PR_isSpaceChar(ch)) {
          nstate = 3;
        }
        if (nstate && pos < i) {
          var t = s.substring(pos, i);
          outlist.push(new PR_Token(t, PR_PLAIN));
          pos = i;
        }
        break;
      case 1:  // identifier state
        if (!PR_isIdentifierPart(ch)) {
          nstate = -1;
        }
        break;
      case 2:  // number literal state
        // handle numeric literals like
        // 0x7f 300UL 100_000

        // this does not treat floating point values as a single literal
        //   0.1 and 3e-6
        // are each split into multiple tokens
        if (!(PR_isDigitChar(ch) || PR_isWordChar(ch) || ch == '_')) {
          nstate = -1;
        }
        break;
      case 3:  // punctuation state
        if (PR_isIdentifierStart(ch) || PR_isDigitChar(ch) ||
            PR_isSpaceChar(ch)) {
          nstate = -1;
        }
        break;
      }
    }

    if (nstate != state) {
      if (nstate < 0) {
        if (i > pos) {
          var t = s.substring(pos, i);
          var ch0 = t.charAt(0);
          var style;
          if (PR_isIdentifierStart(ch0)) {
            if (PR_keywords[t]) {
              style = PR_KEYWORD;
            } else if (ch0 === '@') {
              style = PR_LITERAL;
            } else {
              // Treat any word that starts with an uppercase character and
              // contains at least one lowercase character as a type, or
              // ends with _t.
              // This works perfectly for Java, pretty well for C++, and
              // passably for Python.  The _t catches C structs.
              var isType = false;
              if (ch0 >= 'A' && ch0 <= 'Z') {
                for (var j = 1; j < t.length; j++) {
                  var ch1 = t.charAt(j);
                  if (ch1 >= 'a' && ch1 <= 'z') {
                    isType = true;
                    break;
                  }
                }
                if (!isType && t.length >= 2 &&
                    t.substring(t.length - 2) == '_t') {
                  isType = true;
                }
              }
              style = isType ? PR_TYPE : PR_PLAIN;
            }
          } else if (PR_isDigitChar(ch0)) {
            style = PR_LITERAL;
          } else if (!PR_isSpaceChar(ch0)) {
            style = PR_PUNCTUATION;
          } else {
            style = PR_PLAIN;
          }
          pos = i;
          outlist.push(new PR_Token(t, style));
        }

        state = 0;
        if (nstate == -1) {
          // don't increment.  This allows us to use state 0 to redispatch based
          // on the current character.
          i--;
          continue;
        }
      }
      state = nstate;
    }
  }
}

/** split a group of chunks of markup.
  * @private
  */
function PR_tokenizeMarkup(chunks) {
  if (!(chunks && chunks.length)) { return chunks; }

  var tokenEnds = PR_splitMarkup(chunks);
  return PR_splitChunks(chunks, tokenEnds);
}

/** split tags attributes and their values out from the tag name, and
  * recursively lex source chunks.
  * @private
  */
function PR_splitTagAttributes(tokens) {
  var tokensOut = new Array();
  var state = 0;
  var stateStyle = PR_TAG;
  var delim = null;  // attribute delimiter for quoted value state.
  var decodeHelper = new PR_DecodeHelper();
  for (var ci = 0; ci < tokens.length; ++ci) {
    var tok = tokens[ci];
    if (PR_TAG == tok.style) {
      var s = tok.token;
      var start = 0;
      for (var i = 0; i < s.length; /* i = next at bottom */) {
        decodeHelper.decode(s, i);
        var ch = decodeHelper.ch;
        var next = decodeHelper.next;

        var emitEnd = null;  // null or position of end of chunk to emit.
        var nextStyle = null;  // null or next value of stateStyle
        if (ch == '>') {
          if (PR_TAG != stateStyle) {
            emitEnd = i;
            nextStyle = PR_TAG;
          }
        } else {
          switch (state) {
            case 0:
              if ('<' == ch) { state = 1; }
              break;
            case 1:
              if (PR_isSpaceChar(ch)) { state = 2; }
              break;
            case 2:
              if (!PR_isSpaceChar(ch)) {
                nextStyle = PR_ATTRIB_NAME;
                emitEnd = i;
                state = 3;
              }
              break;
            case 3:
              if ('=' == ch) {
                emitEnd = i;
                nextStyle = PR_TAG;
                state = 5;
              } else if (PR_isSpaceChar(ch)) {
                emitEnd = i;
                nextStyle = PR_TAG;
                state = 4;
              }
              break;
            case 4:
              if ('=' == ch) {
                state = 5;
              } else if (!PR_isSpaceChar(ch)) {
                emitEnd = i;
                nextStyle = PR_ATTRIB_NAME;
                state = 3;
              }
              break;
            case 5:
              if ('"' == ch || '\'' == ch) {
                emitEnd = i;
                nextStyle = PR_ATTRIB_VALUE;
                state = 6;
                delim = ch;
              } else if (!PR_isSpaceChar(ch)) {
                emitEnd = i;
                nextStyle = PR_ATTRIB_VALUE;
                state = 7;
              }
              break;
            case 6:
              if (ch == delim) {
                emitEnd = next;
                nextStyle = PR_TAG;
                state = 2;
              }
              break;
            case 7:
              if (PR_isSpaceChar(ch)) {
                emitEnd = i;
                nextStyle = PR_TAG;
                state = 2;
              }
              break;
          }
        }
        if (emitEnd) {
          if (emitEnd > start) {
            tokensOut.push(
                new PR_Token(s.substring(start, emitEnd), stateStyle));
            start = emitEnd;
          }
          stateStyle = nextStyle;
        }
        i = next;
      }
      if (s.length > start) {
        tokensOut.push(new PR_Token(s.substring(start, s.length), stateStyle));
      }
    } else {
      if (tok.style) {
        state = 0;
        stateStyle = PR_TAG;
      }
      tokensOut.push(tok);
    }
  }
  return tokensOut;
}

/** identify regions of markup that are really source code, and recursivley
  * lex them.
  * @private
  */
function PR_splitSourceNodes(tokens) {
  var tokensOut = new Array();
  // when we see a <script> tag, store '/' here so that we know to end the
  // source processing
  var endScriptTag = null;
  var decodeHelper = new PR_DecodeHelper();

  var sourceChunks = null;

  for (var ci = 0, nc = tokens.length; ci < nc; ++ci) {
    var tok = tokens[ci];
    if (null == tok.style) {
      tokens.push(tok);
      continue;
    }

    var s = tok.token;

    if (null == endScriptTag) {
      if (PR_SOURCE == tok.style) {
        // split off any starting and trailing <?, <%
        if ('<' == decodeHelper.decode(s, 0)) {
          decodeHelper.decode(s, decodeHelper.next);
          if ('%' == decodeHelper.ch || '?' == decodeHelper.ch) {
            endScriptTag = decodeHelper.ch;
            tokensOut.push(new PR_Token(s.substring(0, decodeHelper.next),
                                        PR_TAG));
            s = s.substring(decodeHelper.next, s.length);
          }
        }
      } else if (PR_TAG == tok.style) {
        if ('<' == decodeHelper.decode(s, 0) &&
            '/' != s.charAt(decodeHelper.next)) {
          var tagContent = s.substring(decodeHelper.next).toLowerCase();
          // FIXME(msamuel): this does not mirror exactly the code in
          // in PR_splitMarkup that defers splitting tags inside script and
          // style blocks.
          if (PR_startsWith(tagContent, 'script') ||
              PR_startsWith(tagContent, 'style') ||
              PR_startsWith(tagContent, 'xmp')) {
            endScriptTag = '/';
          }
        }
      }
    }

    if (null != endScriptTag) {
      var endTok = null;
      if (PR_SOURCE == tok.style) {
        if (endScriptTag == '%' || endScriptTag == '?') {
          var pos = s.lastIndexOf(endScriptTag);
          if (pos >= 0 && '>' == decodeHelper.decode(s, pos + 1) &&
              s.length == decodeHelper.next) {
            endTok = new PR_Token(s.substring(pos, s.length), PR_TAG);
            s = s.substring(0, pos);
          }
        }
        if (null == sourceChunks) { sourceChunks = new Array(); }
        sourceChunks.push(new PR_Token(s, PR_PLAIN));
      } else if (PR_PLAIN == tok.style) {
        if (null == sourceChunks) { sourceChunks = new Array(); }
        sourceChunks.push(tok);
      } else if (PR_TAG == tok.style) {
        // if it starts with </ then it must be the end tag.
        if ('<' == decodeHelper.decode(tok.token, 0) &&
            tok.token.length > decodeHelper.next &&
            '/' == decodeHelper.decode(tok.token, decodeHelper.next)) {
          endTok = tok;
        } else {
          tokensOut.push(tok);
        }
      } else {
        if (sourceChunks) {
          sourceChunks.push(tok);
        } else {
          // push remaining tag and attribute tokens from the opening tag
          tokensOut.push(tok);
        }
      }
      if (endTok) {
        if (sourceChunks) {
          var sourceTokens = PR_lexSource(sourceChunks);
          tokensOut.push(new PR_Token('<span class=embsrc>', null));
          for (var si = 0, ns = sourceTokens.length; si < ns; ++si) {
            tokensOut.push(sourceTokens[si]);
          }
          tokensOut.push(new PR_Token('</span>', null));
          sourceChunks = null;
        }
        tokensOut.push(endTok);
        endScriptTag = null;
      }
    } else {
      tokensOut.push(tok);
    }
  }
  return tokensOut;
}

/** splits the quotes from an attribute value.
  * ['"foo"'] -> ['"', 'foo', '"']
  * @private
  */
function PR_splitAttributeQuotes(tokens) {
  var firstPlain = null, lastPlain = null;
  for (var i = 0; i < tokens.length; ++i) {
    if (PR_PLAIN == tokens[i].style) {
      firstPlain = i;
      break;
    }
  }
  for (var i = tokens.length; --i >= 0;) {
    if (PR_PLAIN == tokens[i].style) {
      lastPlain = i;
      break;
    }
  }
  if (null == firstPlain) { return tokens; }

  var decodeHelper = new PR_DecodeHelper();
  var fs = tokens[firstPlain].token;
  var fc = decodeHelper.decode(fs, 0);
  if ('"' != fc && '\'' != fc) {
    return tokens;
  }
  var fpos = decodeHelper.next;

  var ls = tokens[lastPlain].token;
  var lpos = ls.lastIndexOf('&');
  if (lpos < 0) { lpos = ls.length - 1; }
  var lc = decodeHelper.decode(ls, lpos);
  if (lc != fc || decodeHelper.next != ls.length) {
    lc = null;
    lpos = ls.length;
  }

  var tokensOut = new Array();
  for (var i = 0; i < firstPlain; ++i) {
    tokensOut.push(tokens[i]);
  }
  tokensOut.push(new PR_Token(fs.substring(0, fpos), PR_ATTRIB_VALUE));
  if (lastPlain == firstPlain) {
    tokensOut.push(new PR_Token(fs.substring(fpos, lpos), PR_PLAIN));
  } else {
    tokensOut.push(new PR_Token(fs.substring(fpos, fs.length), PR_PLAIN));
    for (var i = firstPlain + 1; i < lastPlain; ++i) {
      tokensOut.push(tokens[i]);
    }
    if (lc) {
      tokens.push(new PR_Token(ls.substring(0, lpos), PR_PLAIN));
    } else {
      tokens.push(tokens[lastPlain]);
    }
  }
  if (lc) {
    tokensOut.push(new PR_Token(ls.substring(lpos, ls.length), PR_PLAIN));
  }
  for (var i = lastPlain + 1; i < tokens.length; ++i) {
    tokensOut.push(tokens[i]);
  }
  return tokensOut;
}

/** identify attribute values that really contain source code and recursively
  * lex them.
  * @private
  */
function PR_splitSourceAttributes(tokens) {
  var tokensOut = new Array();

  var sourceChunks = null;
  var inSource = false;
  var name = '';

  for (var ci = 0, nc = tokens.length; ci < nc; ++ci) {
    var tok = tokens[ci];
    var outList = tokensOut;
    if (PR_TAG == tok.style) {
      if (inSource) {
        inSource = false;
        name = '';
        if (sourceChunks) {
          tokensOut.push(new PR_Token('<span class=embsrc>', null));
          var sourceTokens =
            PR_lexSource(PR_splitAttributeQuotes(sourceChunks));
          for (var si = 0, ns = sourceTokens.length; si < ns; ++si) {
            tokensOut.push(sourceTokens[si]);
          }
          tokensOut.push(new PR_Token('</span>', null));
          sourceChunks = null;
        }
      } else if (name && tok.token.indexOf('=') >= 0) {
        var nameLower = name.toLowerCase();
        if (PR_startsWith(nameLower, 'on') || 'style' == nameLower) {
          inSource = true;
        }
      } else {
        name = '';
      }
    } else if (PR_ATTRIB_NAME == tok.style) {
      name += tok.token;
    } else if (PR_ATTRIB_VALUE == tok.style) {
      if (inSource) {
        if (null == sourceChunks) { sourceChunks = new Array(); }
        outList = sourceChunks;
        tok = new PR_Token(tok.token, PR_PLAIN);
      }
    } else {
      if (sourceChunks) {
        outList = sourceChunks;
      }
    }
    outList.push(tok);
  }
  return tokensOut;
}

/** returns a list of PR_Token objects given chunks of source code.
  *
  * This code assumes that < tokens are html escaped, but " are not.
  * It will do a resonable job with <, but will not recognize an &quot;
  * as starting a string.
  *
  * This code treats ", ', and ` as string delimiters, and \ as a string escape.
  * It does not recognize double delimiter escapes, or perl's qq() style
  * strings.
  *
  * It recognizes C, C++, and shell style comments.
  *
  * @param chunks PR_Tokens with style in (null, PR_PLAIN)
  */
function PR_lexSource(chunks) {
  // positions of ends of tokens in order
  var tokensIn = PR_splitStringAndCommentTokens(chunks);

  // split entities out of so that we know to treat them as single units.
  tokensIn = PR_splitEntities(tokensIn);

  // split non comment|string tokens on whitespace and word boundaries
  var tokensOut = new Array();
  for (var i = 0; i < tokensIn.length; ++i) {
    var tok = tokensIn[i];
    var t = tok.token;
    var s = tok.style;

    if (PR_PLAIN == s) {
      PR_splitNonStringNonCommentToken(t, tokensOut);
      continue;
    }
    tokensOut.push(tok);
  }

  return tokensOut;
}

/** returns a list of PR_Token objects given a string of markup.
  *
  * This code assumes that < tokens are html escaped, but " are not.
  * It will do a resonable job with <, but will not recognize an &quot;
  * as starting a string.
  *
  * This code recognizes a number of constructs.
  * <!-- ... --> comment
  * <!\w ... >   declaration
  * <\w ... >    tag
  * </\w ... >   tag
  * <?...?>      embedded source
  * &[#\w]...;   entity
  *
  * It does not recognizes %foo; entities.
  *
  * It will recurse into any <style>, <script>, and on* attributes using
  * PR_lexSource.
  */
function PR_lexMarkup(chunks) {
  // This function works as follows:
  // 1) Start by splitting the markup into text and tag chunks
  //    Input:  String s
  //    Output: List<PR_Token> where style in (PR_PLAIN, null)
  // 2) Then split the text chunks further into comments, declarations,
  //    tags, etc.
  //    After each split, consider whether the token is the start of an
  //    embedded source section, i.e. is an open <script> tag.  If it is,
  //    find the corresponding close token, and don't bother to lex in between.
  //    Input:  List<String>
  //    Output: List<PR_Token> with style in (PR_TAG, PR_PLAIN, PR_SOURCE, null)
  // 3) Finally go over each tag token and split out attribute names and values.
  //    Input:  List<PR_Token>
  //    Output: List<PR_Token> where style in
  //            (PR_TAG, PR_PLAIN, PR_SOURCE, NAME, VALUE, null)
  var tokensOut = PR_tokenizeMarkup(chunks);
  tokensOut = PR_splitTagAttributes(tokensOut);
  tokensOut = PR_splitSourceNodes(tokensOut);
  tokensOut = PR_splitSourceAttributes(tokensOut);
  return tokensOut;
}

/** classify the string as either source or markup and lex appropriately. */
function PR_lexOne(s) {
  var chunks = PR_chunkify(s);
  // treat it as markup if the first non whitespace character is a < and the
  // last non-whitespace character is a >
  var isMarkup = false;
  for (var i = 0; i < chunks.length; ++i) {
    if (PR_PLAIN == chunks[i].style) {
      if (PR_startsWith(PR_trim(chunks[i].token), '&lt;')) {
        for (var j = chunks.length; --j >= 0;) {
          if (PR_PLAIN == chunks[j].style) {
            isMarkup = PR_endsWith(PR_trim(chunks[j].token), '&gt;');
            break;
          }
        }
      }
      break;
    }
  }
  return isMarkup ? PR_lexMarkup(chunks) : PR_lexSource(chunks);
}

/** pretty print a chunk of code.
  *
  * @param s code as html
  * @return code as html, but prettier
  */
function prettyPrintOne(s) {
  try {
    var tokens = PR_lexOne(s);
    var out = [];
    var lastStyle = null;
    for (var i = 0; i < tokens.length; i++) {
      var t = tokens[i];
      if (t.style != lastStyle) {
        if (lastStyle != null) {
          out.push('</span>');
        }
        if (t.style != null) {
          out.push('<span class=', t.style, '>');
        }
        lastStyle = t.style;
      }
      var html = t.token;
      if (null != t.style) {
        // This interacts badly with some wikis which introduces paragraph tags
        // into pre blocks for some strange reason.
        // It's necessary for IE though which seems to lose the preformattedness
        // of <pre> tags when their innerHTML is assigned.
        // http://stud3.tuwien.ac.at/~e0226430/innerHtmlQuirk.html
        html = html
               .replace(/(\r\n?|\n| ) /g, '$1&nbsp;')
               .replace(/\r\n?|\n/g, '<br>');
      }
      out.push(html);
    }
    if (lastStyle != null) {
      out.push('</span>');
    }
    return out.join('');
  } catch (e) {
    //alert(e.stack);  // DISABLE in production
    return s;
  }
}

/** find all the < pre > and < code > tags in the DOM with class=prettyprint and
  * prettify them.
  */
function prettyPrint() {
  // fetch a list of nodes to rewrite
  var codeSegments = [
      document.getElementsByTagName('pre'),
      document.getElementsByTagName('code'),
      document.getElementsByTagName('xmp') ];
  var elements = [];
  for (var i = 0; i < codeSegments.length; ++i) {
    for (var j = 0; j < codeSegments[i].length; ++j) {
      elements.push(codeSegments[i][j]);
    }
  }
  codeSegments = null;

  // the loop is broken into a series of continuations to make sure that we
  // don't make the browser unresponsive when rewriting a large page.
  var k = 0;

  function doWork() {
    var endTime = new Date().getTime() + 250;
    for (; k < elements.length && new Date().getTime() < endTime; k++) {
      var cs = elements[k];
      if (cs.className && cs.className.indexOf('prettyprint') >= 0) {

        // make sure this is not nested in an already prettified element
        var nested = false;
        for (var p = cs.parentNode; p != null; p = p.parentNode) {
          if ((p.tagName == 'pre' || p.tagName == 'code' ||
               p.tagName == 'xmp') &&
              p.className && p.className.indexOf('prettyprint') >= 0) {
            nested = true;
            break;
          }
        }
        if (!nested) {
          // fetch the content as a snippet of properly escaped HTML.
          // Firefox adds newlines at the end.
          var content = PR_getInnerHtml(cs);
          content = content.replace(/(?:\r\n?|\n)$/, '');
          if (!content) { continue; }

          // do the pretty printing
          var newContent = prettyPrintOne(content);

          // push the prettified html back into the tag.
          if (!PR_isRawContent(cs)) {
            // just replace the old html with the new
            cs.innerHTML = newContent;
          } else {
            // we need to change the tag to a <pre> since <xmp>s do not allow
            // embedded tags such as the span tags used to attach styles to
            // sections of source code.
            var pre = document.createElement('PRE');
            for (var i = 0; i < cs.attributes.length; ++i) {
              var a = cs.attributes[i];
              if (a.specified) {
                pre.setAttribute(a.name, a.value);
              }
            }
            pre.innerHTML = newContent;
            // remove the old
            cs.parentNode.replaceChild(pre, cs);
          }
        }
      }
    }
    if (k < elements.length) {
      // finish up in a continuation
      setTimeout(doWork, 250);
    }
  }

  doWork();
}
