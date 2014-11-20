#!/usr/bin/perl -w

use strict;
use warnings;
use geobase;

foreach my $key (keys %Region) {
	print $key . "\t"
		. ($Region{$key}->{parents}[-1] || 0) . "\t"
		. ($Region{$key}->{type} || 0) . "\t"
		. (int($Region{$key}->{zip_old} || 0)) . "\n";	# zip_old, не смотря на название, содержит население региона.
}
