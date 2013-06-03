#!/usr/bin/perl -w

use strict;
use warnings;
use geobase;

my @languages = ('ru', 'en', 'ua', 'by', 'kz', 'tr');
my @output_files = map { open(my $output, ">:encoding(UTF-8)", "regions_names_" . $_ . ".txt") || die $!; $output } @languages;
my %outputs;
@outputs{@languages} = @output_files;

foreach my $key (keys %Region) {
	foreach my $lang (@languages) {
		my $field = ( $lang eq 'ru' ? 'name' : $lang . '_name' );
		my $name = $Region{$key}->{$field};
		if ($name) {
			print { $outputs{$lang} } $key . "\t" . $name . "\n";
		}
	}	
}
