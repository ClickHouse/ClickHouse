#!/usr/bin/env perl

#
# Generate some random sql queries
#

package sqlfuzz;
use 5.16.0;
use strict;
use warnings;
no if $] >= 5.017011, warnings => 'experimental::smartmatch';
use Data::Dumper;

sub shuffle(@) {    #@$deck = map{ splice @$deck, rand(@$deck),  1 }  0..$#$deck;
    my $deck = shift;
    $deck = [$deck, @_] unless ref $deck eq 'ARRAY';
    my $i = @$deck;
    while ($i--) {
        my $j = int rand($i + 1);
        @$deck[$i, $j] = @$deck[$j, $i];
    }
    return wantarray ? @$deck : $deck;
}

sub rand_pick ($) {
    my ($arr) = @_;
    return $arr->[rand(@$arr)];
}

sub rand_word {
    my ($l) = shift || 8;
    my $minl = 1;
    @_ = ('a' .. 'z') unless @_;
    join '', @_[map { rand @_ } $minl .. $minl + rand($l)];
}

sub rand_string {
    my ($w) = shift || 5;
    join ' ', map { rand_word((), @_) } 1 .. rand($w);
}

sub one ($$) {
    my ($state, $value) = @_;
    return ref $value ~~ 'CODE' ? $value->() : $value;
}

sub one_of ($$) {
    my ($state, $hash) = @_;
    #state $last_selected;
    #my $last_n = $last_selected->{"$hash"};
    my $value;
    if ('ARRAY' ~~ ref $hash) {
        $value = rand_pick $hash;
    } else {
        my $keys_array = [sort keys %$hash];
        $value = $hash->{rand_pick $keys_array};
    }

#$last_selected->{"$hash"} = !exists $last_selected->{"$hash"} ? 0 : $last_selected->{"$hash"} < @keys_array - 1 ? $last_selected->{"$hash"} + 1 : 0;
    ##warn join ' : ',$last_selected->{"$hash"}, $keys_array[$last_selected->{"$hash"}], $hash->{$keys_array[$last_selected->{"$hash"}]};
    #my $value = $hash->{$keys_array[$last_selected->{"$hash"}]};

    return one($state, $value);
}

sub list_of ($$@) {
    my ($state, $opt) = (shift, shift);
    my $max = ($opt->{min} // 1) + int rand($opt->{max} || 4);
    my @ret;
    while (@ret < $max) {
        for my $hash (shuffle \@_) {
            push @ret, one_of($state, $hash);
        }
    }
    return join ', ', @ret;
}

sub file_read ($) {
    open my $f, '<', $_[0] or return;
    local $/ = undef;
    my $ret = <$f>;
    close $f;
    return $ret;
}

our ($query, $query_select, $expression_cast, $expression, $type, $type_cast, $functions, $table_functions);
$type_cast = {map { $_ => $_ } qw(DateTime Date String)};
$type = {%$type_cast, (map { $_ => $_ } qw(Int8 Int16 Int32 Int64 UInt8 UInt16 UInt32 UInt64 Float32 Float64))};
$type->{"Nullable($_)"} = "Nullable($_)" for values %$type;
# AS, LIKE, NOT LIKE, IN, NOT IN, GLOBAL IN, GLOBAL NOT IN, BETWEEN, IS, ClosingRoundBracket, Comma, Dot, Arrow, QuestionMark, OR, AND

$expression_cast = {
    'CAST' => sub { my ($state) = @_; '(CAST((' . one_of($state, $expression_cast) . ') AS ' . one_of($state, $type_cast) . '))' },
    'SELECT' => sub {
        my ($state) = @_;
        list_of(
            $state, {max => 2},
            #[sub { '( ' . $query->{SELECT}->() . ' ) AS ' . rand_word() }, sub { '( ' . $query->{SELECT}->() . ' ) ' }]
            [sub { '( ' . one_of($state, $query_select) . ' ) AS ' . rand_word() }, sub { '( ' . one_of($state, $query_select) . ' ) ' }]
        );
    },
    'number' => sub { my ($state) = @_; return rand_pick(['', '-']) . rand_word(8, 0 .. 9) . rand_pick(['', '.' . rand_word(6, 0 .. 9)]) },
    'string' => sub {
        my ($state) = @_;
        return q{'} . rand_word(8, map { $_ ~~ q{'} ? '\\' . $_ : $_ } map {chr} 32 .. 127) . q{'};
    },
    '[]'  => '[]',
    '[x]' => sub { my ($state) = @_; return '[' . one_of($state, $expression) . ']' },
    'function()' =>
      sub { my ($state) = @_; return one_of($state, $functions) . '(' . list_of($state, {min => 0, max => 3}, $expression) . ')' },
    "'\\0'" => "'\\0'",
    "''"    => "''",
    'NULL'  => 'NULL',
};
$expression = {
    %$expression_cast,
};
$query_select = {
    'SELECT' => sub { my ($state) = @_; return 'SELECT ' . list_of($state, {max => 5}, $expression) },
    'SELECT function()' => sub { my ($state) = @_; return 'SELECT ' . one($state, $expression->{'function()'}) },
    'SELECT table_function()' => sub {
        my ($state) = @_;
        return 'SELECT * FROM ' . one_of($state, $table_functions) . '(' . list_of($state, {min => 0, max => 3}, $expression) . ')';
    },
};

$query = {%$query_select};

sub main {
    srand($ENV{SQL_FUZZY_SRAND} + $ENV{SQL_FUZZY_RUN}) if $ENV{SQL_FUZZY_SRAND};
    # select name from system.functions format TSV;
    $functions = [
        split /[\s;,]+/,
        $ENV{SQL_FUZZY_FUNCTIONS}
          || file_read($ENV{SQL_FUZZY_FILE_FUNCTIONS} || 'clickhouse-functions')
    ];
    # $functions = [grep { not $_ ~~ [qw( )] } @$functions];    # will be removed
    # select name from system.table_functions format TSV;
    $table_functions = [
        split /[\s;,]+/,
        $ENV{SQL_FUZZY_TABLE_FUNCTIONS}
          || file_read($ENV{SQL_FUZZY_FILE_TABLE_FUNCTIONS} || 'clickhouse-table-functions')
    ];
    $table_functions = [grep { not $_ ~~ [qw(numbers)] } @$table_functions];    # too slow
    say one_of({}, $query), ';' for 1 .. ($ENV{SQL_FUZZY_LINES} || 100);
}

main() unless caller;

#say rand_word() for 1..10000;
#say rand_word(8, 0..9) for 1..1000;

