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
          || '__inner_restore_projection__ __inner_build_projection_composition__ convertCharset one_or_zero findClusterValue findClusterIndex toNullable coalesce isNotNull pointInEllipses transform pow acos asin tan cos tgamma lgamma erfc erf sqrt log10 exp10 e visitParamExtractFloat visitParamExtractUInt decodeURLComponent cutURLParameter cutQueryStringAndFragment cutFragment cutWWW URLPathHierarchy URLHierarchy extractURLParameterNames extractURLParameter queryStringAndFragment pathFull sin topLevelDomain domainWithoutWWW domain protocol greatCircleDistance extract match positionCaseInsensitiveUTF8 positionCaseInsensitive positionUTF8 position replaceRegexpAll replaceRegexpOne arrayStringConcat splitByString splitByChar alphaTokens endsWith startsWith appendTrailingCharIfAbsent substringUTF8 concatAssumeInjective reverseUTF8 upperUTF8 __inner_project__ upper lower length notEmpty trunc round roundAge roundDuration roundToExp2 reinterpretAsString reinterpretAsDateTime reinterpretAsDate reinterpretAsFloat64 reinterpretAsFloat32 reinterpretAsInt64 reinterpretAsInt8 reinterpretAsUInt32 toStartOfFiveMinute toISOYear toISOWeek concat toDecimal64 ifNull toStartOfDay toSecond addSeconds sleepEachRow materialize visitParamExtractInt toStartOfMinute toDayOfWeek toDayOfMonth bitShiftLeft emptyArrayUInt8 parseDateTimeBestEffort toTime toDateTimeOrNull toFloat32OrNull toInt16 IPv6NumToString atan substring arrayIntersect isInfinite toRelativeHourNum hex arrayEnumerateDense toUInt8OrZero toRelativeSecondNum toUInt64OrNull MACNumToString toInt32OrNull toDayOfYear toUnixTimestamp toString toDateOrZero subtractDays toMinute murmurHash3_64 murmurHash2_32 toUInt64 toUInt8 dictGetDateTime empty isFinite caseWithoutExpression caseWithoutExpr visitParamExtractRaw queryString dictGetInt32OrDefault caseWithExpression toInt8OrZero multiIf if intExp10 bitShiftRight less toUInt8OrNull toInt8OrNull bitmaskToArray toIntervalYear toFloat64OrZero dateDiff generateUUIDv4 arrayPopBack toIntervalMonth toUUID notEquals toInt16OrNull murmurHash2_64 hasAny toIntervalMinute isNull tupleElement replaceAll parseDateTimeBestEffortOrZero toFloat32OrZero lowerUTF8 notIn gcd like regionToPopulation MACStringToOUI notLike toStringCutToZero lcm parseDateTimeBestEffortOrNull not toInt32OrZero arrayFilter toInt16OrZero range equals now toTypeName toUInt32OrNull emptyArrayString dictGetDateTimeOrDefault bitRotateRight cutIPv6 toUInt32OrZero timezone reverse runningDifferenceStartingWithFirstValue toDateTime arrayPopFront toInt32 intHash64 extractURLParameters lowCardinalityIndices toStartOfMonth toYear hasAll rowNumberInAllBlocks bitTestAll arrayCount arraySort abs bitNot intDiv intDivOrZero firstSignificantSubdomain dictGetFloat32OrDefault reinterpretAsUInt16 toHour minus regionToArea unhex IPv4StringToNum toIntervalHour toInt8 dictGetFloat32 log IPv4NumToString modulo arrayEnumerate cutQueryString reinterpretAsFixedString countEqual bitTest toDecimal128 plus or reinterpretAsUInt64 toMonth visitParamExtractBool emptyArrayUInt64 replaceOne arrayReverseSort toFloat32 toRelativeMonthNum emptyArrayInt32 toRelativeYearNum arrayElement log2 array arrayReverse toUInt64OrZero emptyArrayFloat64 negate arrayPushBack subtractWeeks bitTestAny bitAnd toDecimal32 arrayPushFront lessOrEquals intExp2 toUInt16OrZero arrayConcat arrayCumSum arraySlice addDays dictGetUInt8 toUInt32 bitOr caseWithExpr toStartOfYear toIntervalDay MD5 emptyArrayUInt32 emptyArrayInt8 toMonday addMonths arrayUniq SHA256 arrayExists multiply toUInt16OrNull dictGetInt8 visitParamHas emptyArrayInt64 toIntervalSecond toDate sleep emptyArrayToSingle path toInt64OrZero SHA1 extractAll emptyArrayDate dumpColumnStructure toInt64 lengthUTF8 greatest arrayEnumerateUniq arrayDistinct arrayFirst toFixedString IPv4NumToStringClassC toFloat64OrNull IPv4ToIPv6 identity ceil toStartOfQuarter dictGetInt8OrDefault MACStringToNum emptyArrayUInt16 UUIDStringToNum dictGetUInt16 toStartOfFifteenMinutes toStartOfHour sumburConsistentHash toStartOfISOYear toRelativeQuarterNum toRelativeWeekNum toRelativeDayNum cbrt yesterday bitXor timeSlot timeSlots emptyArrayInt16 dictGetInt16 toYYYYMM toYYYYMMDDhhmmss toUInt16 addMinutes addHours addWeeks nullIf subtractSeconds subtractMinutes toIntervalWeek subtractHours isNaN subtractMonths toDateOrNull subtractYears toTimeZone formatDateTime has cityHash64 intHash32 fragment regionToCity indexOf regionToDistrict regionToCountry visibleWidth regionToContinent regionToTopContinent toColumnTypeName regionHierarchy CHAR_LENGTH least divide SEHierarchy dictGetDate OSToRoot SEToRoot OSIn SEIn regionToName dictGetStringOrDefault OSHierarchy exp floor dictGetUInt8OrDefault dictHas dictGetUInt64 cutToFirstSignificantSubdomain dictGetInt32 pointInPolygon dictGetInt64 blockNumber IPv6StringToNum dictGetString dictGetFloat64 dictGetUUID CHARACTER_LENGTH toQuarter dictGetHierarchy toFloat64 arraySum toInt64OrNull dictIsIn dictGetUInt16OrDefault dictGetUInt32OrDefault emptyArrayDateTime greater jumpConsistentHash dictGetUInt64OrDefault dictGetInt16OrDefault dictGetInt64OrDefault reinterpretAsInt32 dictGetUInt32 murmurHash3_32 bar dictGetUUIDOrDefault rand modelEvaluate arrayReduce farmHash64 bitmaskToList formatReadableSize halfMD5 SHA224 arrayMap sipHash64 dictGetFloat64OrDefault sipHash128 metroHash64 murmurHash3_128 yandexConsistentHash emptyArrayFloat32 arrayAll toYYYYMMDD today arrayFirstIndex greaterOrEquals arrayDifference visitParamExtractString toDateTimeOrZero globalNotIn throwIf and xor currentDatabase hostName URLHash getSizeOfEnumType defaultValueOfArgumentType blockSize tuple arrayCumSumNonNegative rowNumberInBlock arrayResize ignore toRelativeMinuteNum indexHint reinterpretAsInt16 addYears arrayJoin replicate hasColumnInTable version regionIn uptime runningAccumulate runningDifference assumeNotNull pi finalizeAggregation toLowCardinality exp2 lowCardinalityKeys in globalIn dictGetDateOrDefault rand64 CAST bitRotateLeft randConstant UUIDNumToString reinterpretAsUInt8 truncate ceiling retention maxIntersections groupBitXor groupBitOr uniqUpTo uniqCombined uniqExact uniq covarPop stddevPop varPop covarSamp varSamp sumMap corrStable corr quantileTiming quantileDeterministic quantilesExact uniqHLL12 quantilesTiming covarPopStable stddevSampStable quantilesExactWeighted quantileExactWeighted quantileTimingWeighted quantileExact quantilesDeterministic quantiles topK sumWithOverflow count groupArray stddevSamp groupArrayInsertAt quantile quantilesTimingWeighted quantileTDigest quantilesTDigest windowFunnel min argMax varSampStable maxIntersectionsPosition quantilesTDigestWeighted groupUniqArray sequenceCount sumKahan any anyHeavy histogram quantileTDigestWeighted max groupBitAnd argMin varPopStable avg sequenceMatch stddevPopStable sum anyLast covarSampStable BIT_XOR medianExactWeighted medianTiming medianExact median medianDeterministic VAR_SAMP STDDEV_POP medianTDigest VAR_POP medianTDigestWeighted BIT_OR STDDEV_SAMP medianTimingWeighted COVAR_SAMP COVAR_POP BIT_AND'
    ];
    # $functions = [grep { not $_ ~~ [qw( )] } @$functions];    # will be removed
    # select name from system.table_functions format TSV;
    $table_functions = [
        split /[\s;,]+/,
        $ENV{SQL_FUZZY_TABLE_FUNCTIONS}
          || file_read($ENV{SQL_FUZZY_FILE_TABLE_FUNCTIONS} || 'clickhouse-table-functions')
          || 'mysql jdbc odbc remote catBoostPool merge file cluster shardByHash url numbers'
    ];
    $table_functions = [grep { not $_ ~~ [qw(numbers)] } @$table_functions];    # too slow
    say one_of({}, $query), ';' for 1 .. ($ENV{SQL_FUZZY_LINES} || 100);
}

main() unless caller;

#say rand_word() for 1..10000;
#say rand_word(8, 0..9) for 1..1000;

