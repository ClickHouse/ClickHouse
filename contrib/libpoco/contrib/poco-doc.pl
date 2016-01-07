#!/usr/bin/perl -wi
#
# poco-doc.pl
#
# This script, when run on a POCO header file, moves the documentation
# for classes, methods, etc above their declarations, making the code
# suitable for running through Doxygen, etc.
#
# Author: Caleb Epstein <caleb.epstein@gmail.com>
#
# $Id$

use strict;
use warnings;

my @COMMENT;
my @DECL;

my $comment_re = qr@^\s*//@;

while (<>) {
   if ((/^\s*(template|class|enum)/ and not /\;\s*$/) or
       (/[\(\)](\s*const)?\;$/ and $_ !~ $comment_re)) {
      if (scalar @DECL) {
         print join ("", @COMMENT) if scalar @COMMENT;
         print join ("", @DECL);
      }
      @DECL = ($_);
      @COMMENT = ();
      next;
   } elsif (m@^\s*///@ and scalar @DECL) {
      push (@COMMENT, $_);
   } else {
      if (scalar @DECL) {
         print join ("", @COMMENT) if scalar @COMMENT;
         print join ("", @DECL);
         @COMMENT = @DECL = ();
      }

      # Handle in-line documentation of enum values
      if (m@^\s*[^/]@ and m@/// @) {
         s@/// @///< @;
      }
      print;
   }
}

