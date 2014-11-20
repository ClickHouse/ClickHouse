#!/usr/bin/perl -w

use strict;
use warnings;
use geobase;


sub get_population {
	my $key = shift;
	my $depth = shift || 0;

	return 0 if ($depth > 100);

	my $current = int($Region{$key}->{zip_old} || 0); # zip_old, не смотря на название, содержит население региона.
	return $current if ($current);

	my $sum_of_children = 0;
	for my $child (@{$Region{$key}->{chld}}) {
		$sum_of_children += get_population($child, $depth + 1);
	}

	return $sum_of_children;
}


foreach my $key (keys %Region) {
	print $key . "\t"
		. ($Region{$key}->{parents}[-1] || 0) . "\t"
		. ($Region{$key}->{type} || 0) . "\t"
		. get_population($key) . "\n";
}
