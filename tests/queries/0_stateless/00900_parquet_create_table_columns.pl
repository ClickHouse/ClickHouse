#!/usr/bin/env perl
package parquet_create_table_columns;
use strict;
no warnings 'experimental';
use feature 'signatures';
use JSON::XS;
#use Data::Dumper;

sub file_read($file) {
    open my $f, '<', $file or return;
    local $/ = undef;
    my $ret = <$f>;
    close $f;
    return $ret;
}

our $type_parquet_logical_to_clickhouse = {
    DECIMAL    => 'Decimal128(1)',
    TIMESTAMP_MICROS => 'DateTime',
    TIMESTAMP_MILLIS => 'DateTime',
};
our $type_parquet_physical_to_clickhouse = {
    BOOLEAN    => 'UInt8',
    INT32      => 'Int32',
    INT64      => 'Int64',
    FLOAT      => 'Float32',
    DOUBLE     => 'Float64',
    BYTE_ARRAY => 'String',
    FIXED_LEN_BYTE_ARRAY => 'String', # Maybe FixedString?
    INT96      => 'Int64',     # TODO!
};

sub columns ($json) {
    my @list;
    my %uniq;
    for my $column (@{$json->{Columns}}) {
        #warn Data::Dumper::Dumper $column;
        my $name = $column->{'Name'};
        my $type = $type_parquet_logical_to_clickhouse->{$column->{'LogicalType'}} || $type_parquet_physical_to_clickhouse->{$column->{'PhysicalType'}};
        unless ($type) {
            warn "Unknown type [$column->{'PhysicalType'}:$column->{'LogicalType'}] of column [$name]";
        }
        $type = "Nullable($type)";
        $name .= $column->{'Id'} if $uniq{$name}++; # Names can be non-unique
        push @list, {name => $name, type => $type};
    }
    print join ', ', map {"`$_->{name}` $_->{type}"} @list;
}

sub columns_file ($file) {
    return columns(JSON::XS::decode_json(file_read($file)));
}

columns_file(shift) unless caller;
