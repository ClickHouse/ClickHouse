#!/usr/bin/env perl
package parquet_create_table_columns;
use strict;
no warnings 'experimental';
use feature 'signatures';
use JSON;
#use Data::Dumper;

sub file_read($file) {
    open my $f, '<', $file or return;
    local $/ = undef;
    my $ret = <$f>;
    close $f;
    return $ret;
}

our $type_parquet_to_clickhouse = {
    BOOLEAN    => 'UInt8',
    INT32      => 'Int32',
    INT64      => 'Int64',
    FLOAT      => 'Float32',
    DOUBLE     => 'Float64',
    BYTE_ARRAY => 'String',
    INT96      => 'Int64',     # TODO!
};

sub columns ($json) {
    my @list;
    my %uniq;
    for my $column (@{$json->{Columns}}) {
        #warn Data::Dumper::Dumper $column;
        my $name = $column->{'Name'};
        unless (exists $type_parquet_to_clickhouse->{$column->{'PhysicalType'}}) {
            warn "Unknown type [$column->{'PhysicalType'}] of column [$name]";
        }
        $name .= $column->{'Id'} if $uniq{$name}++; # Names can be non-unique
        push @list, {name => $name, type => $type_parquet_to_clickhouse->{$column->{'PhysicalType'}}};
    }
    print join ', ', map {"$_->{name} $_->{type}"} @list;
}

sub columns_file ($file) {
    return columns(JSON::decode_json(file_read($file)));
}

columns_file(shift) unless caller;
