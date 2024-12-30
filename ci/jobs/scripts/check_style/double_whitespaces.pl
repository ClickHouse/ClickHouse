#!/usr/bin/perl

use strict;

# Find double whitespace such as "a,  b, c" that looks very ugly and annoying.
# But skip double whitespaces if they are used as an alignment - by comparing to surrounding lines.

my $ret = 0;

foreach my $file (@ARGV)
{
    my @array;

    open (FH,'<',$file);
    while (<FH>)
    {
        push @array, $_;
    }

    for (my $i = 1; $i < $#array; ++$i)
    {
        if ($array[$i] =~ ',( {2,3})[^ /]')
        {
            # https://stackoverflow.com/questions/87380/how-can-i-find-the-location-of-a-regex-match-in-perl

            if ((substr($array[$i - 1], $+[1] - 1, 2) !~ /^[ -][^ ]$/) # whitespaces are not part of alignment
             && (substr($array[$i + 1], $+[1] - 1, 2) !~ /^[ -][^ ]$/)
             && $array[$i] !~ /(-?\d+\w*,\s+){3,}/) # this is not a number table like { 10, -1,  2 }
            {
                print($file . ":" . ($i + 1) . $array[$i]);
                $ret = 1;
            }
        }
    }
}

exit $ret;
