/* Terminal color manipulation macros.
   Copyright (C) 2005-2018 Free Software Foundation, Inc.

This file is part of GCC.

GCC is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free
Software Foundation; either version 3, or (at your option) any later
version.

GCC is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
for more details.

You should have received a copy of the GNU General Public License
along with GCC; see the file COPYING3.  If not see
<http://www.gnu.org/licenses/>.  */

#ifndef GCC_COLOR_MACROS_H
#define GCC_COLOR_MACROS_H

/* Select Graphic Rendition (SGR, "\33[...m") strings.  */
/* Also Erase in Line (EL) to Right ("\33[K") by default.  */
/*    Why have EL to Right after SGR?
	 -- The behavior of line-wrapping when at the bottom of the
	    terminal screen and at the end of the current line is often
	    such that a new line is introduced, entirely cleared with
	    the current background color which may be different from the
	    default one (see the boolean back_color_erase terminfo(5)
	    capability), thus scrolling the display by one line.
	    The end of this new line will stay in this background color
	    even after reverting to the default background color with
	    "\33[m', unless it is explicitly cleared again with "\33[K"
	    (which is the behavior the user would instinctively expect
	    from the whole thing).  There may be some unavoidable
	    background-color flicker at the end of this new line because
	    of this (when timing with the monitor's redraw is just right).
	 -- The behavior of HT (tab, "\t") is usually the same as that of
	    Cursor Forward Tabulation (CHT) with a default parameter
	    of 1 ("\33[I"), i.e., it performs pure movement to the next
	    tab stop, without any clearing of either content or screen
	    attributes (including background color); try
	       printf 'asdfqwerzxcv\rASDF\tZXCV\n'
	    in a bash(1) shell to demonstrate this.  This is not what the
	    user would instinctively expect of HT (but is ok for CHT).
	    The instinctive behavior would include clearing the terminal
	    cells that are skipped over by HT with blank cells in the
	    current screen attributes, including background color;
	    the boolean dest_tabs_magic_smso terminfo(5) capability
	    indicates this saner behavior for HT, but only some rare
	    terminals have it (although it also indicates a special
	    glitch with standout mode in the Teleray terminal for which
	    it was initially introduced).  The remedy is to add "\33K"
	    after each SGR sequence, be it START (to fix the behavior
	    of any HT after that before another SGR) or END (to fix the
	    behavior of an HT in default background color that would
	    follow a line-wrapping at the bottom of the screen in another
	    background color, and to complement doing it after START).
	    Piping GCC's output through a pager such as less(1) avoids
	    any HT problems since the pager performs tab expansion.

      Generic disadvantages of this remedy are:
	 -- Some very rare terminals might support SGR but not EL (nobody
	    will use "gcc -fdiagnostics-color" on a terminal that does not
	    support SGR in the first place).
	 -- Having these extra control sequences might somewhat complicate
	    the task of any program trying to parse "gcc -fdiagnostics-color"
	    output in order to extract structuring information from it.
      A specific disadvantage to doing it after SGR START is:
	 -- Even more possible background color flicker (when timing
	    with the monitor's redraw is just right), even when not at the
	    bottom of the screen.
      There are no additional disadvantages specific to doing it after
      SGR END.

      It would be impractical for GCC to become a full-fledged
      terminal program linked against ncurses or the like, so it will
      not detect terminfo(5) capabilities.  */

#define COLOR_SEPARATOR		";"
#define COLOR_NONE		"00"
#define COLOR_BOLD		"01"
#define COLOR_UNDERSCORE	"04"
#define COLOR_BLINK		"05"
#define COLOR_REVERSE		"07"
#define COLOR_FG_BLACK		"30"
#define COLOR_FG_RED		"31"
#define COLOR_FG_GREEN		"32"
#define COLOR_FG_YELLOW		"33"
#define COLOR_FG_BLUE		"34"
#define COLOR_FG_MAGENTA	"35"
#define COLOR_FG_CYAN		"36"
#define COLOR_FG_WHITE		"37"
#define COLOR_BG_BLACK		"40"
#define COLOR_BG_RED		"41"
#define COLOR_BG_GREEN		"42"
#define COLOR_BG_YELLOW		"43"
#define COLOR_BG_BLUE		"44"
#define COLOR_BG_MAGENTA	"45"
#define COLOR_BG_CYAN		"46"
#define COLOR_BG_WHITE		"47"
#define SGR_START		"\33["
#define SGR_END			"m\33[K"
#define SGR_SEQ(str)		SGR_START str SGR_END
#define SGR_RESET		SGR_SEQ("")

#endif  /* GCC_COLOR_MACROS_H */
