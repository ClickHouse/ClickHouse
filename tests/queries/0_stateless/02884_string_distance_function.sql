SELECT '-- const arguments';
-- just to see it works
SELECT 'clickhouse' AS s1, 'mouse' AS s2, byteHammingDistance(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, editDistance(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, damerauLevenshteinDistance(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, stringJaccardIndex(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, stringJaccardIndexUTF8(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, jaroSimilarity(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, jaroWinklerSimilarity(s1, s2);

SELECT '-- test aliases';
SELECT 'clickhouse' AS s1, 'mouse' AS s2, mismatches(s1, s2);
SELECT 'clickhouse' AS s1, 'mouse' AS s2, levenshteinDistance(s1, s2);

SELECT '-- Deny DoS using too large inputs';
SELECT editDistance(randomString(power(2, 17)), 'abc'); -- { serverError TOO_LARGE_STRING_SIZE}
SELECT damerauLevenshteinDistance(randomString(power(2, 17)), 'abc'); -- { serverError TOO_LARGE_STRING_SIZE}
SELECT jaroSimilarity(randomString(power(2, 17)), 'abc'); -- { serverError TOO_LARGE_STRING_SIZE}
SELECT jaroWinklerSimilarity(randomString(power(2, 17)), 'abc'); -- { serverError TOO_LARGE_STRING_SIZE}

DROP TABLE IF EXISTS t;
CREATE TABLE t
(
    s1 String,
    s2 String
) ENGINE = MergeTree ORDER BY s1;

-- actual test cases
INSERT INTO t VALUES ('', '') ('abc', '') ('', 'abc') ('abc', 'abc') ('abc', 'ab') ('abc', 'bc') ('clickhouse', 'mouse') ('我是谁', 'Tom') ('Jerry', '我是谁') ('我是谁', '我是我');

SELECT '-- non-const arguments';
SELECT 'byteHammingDistance', s1, s2, byteHammingDistance(s1, s2) FROM t ORDER BY ALL;
SELECT 'editDistance', s1, s2, editDistance(s1, s2) FROM t ORDER BY ALL;
SELECT 'editDistanceUTF8', s1, s2, editDistanceUTF8(s1, s2) FROM t ORDER BY ALL;
SELECT 'damerauLevenshteinDistance', s1, s2, damerauLevenshteinDistance(s1, s2) FROM t ORDER BY ALL;
SELECT 'stringJaccardIndex', s1, s2, stringJaccardIndex(s1, s2) FROM t ORDER BY ALL;
SELECT 'stringJaccardIndexUTF8', s1, s2, stringJaccardIndexUTF8(s1, s2) FROM t ORDER BY ALL;
SELECT 'jaroSimilarity', s1, s2, jaroSimilarity(s1, s2) FROM t ORDER BY ALL;
SELECT 'jaroWinklerSimilarity', s1, s2, jaroWinklerSimilarity(s1, s2) FROM t ORDER BY ALL;

SELECT '-- Special UTF-8 tests';
-- We do not perform full UTF8 validation, so sometimes it just returns some result
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\x48\x65\x6C'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xFF\xFF\xFF\xFF'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\x41\xE2\x82\xAC'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xF0\x9F\x99\x82'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xFF'));
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xC2\x01')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xC1\x81')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xF0\x80\x80\x41')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xC0\x80')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xD8\x00 ')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8(materialize('hello'), materialize('\xDC\x00')); -- { serverError BAD_ARGUMENTS }
SELECT stringJaccardIndexUTF8('😃🌍', '🙃😃🌑'), stringJaccardIndex('😃🌍', '🙃😃🌑');

-- Lorem ipsum
-- Chinese chars lorem ipsum
SELECT stringJaccardIndex(
    materialize('Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin quis mauris enim. Nam venenatis nunc.'),
    materialize('tenier niczuje polecz udzian telicy urostak st go sie osoby zaktylko moce o rawia si wora bywia nialic'));
SELECT stringJaccardIndexUTF8(
    materialize('abcd寸筆辛國喜吹。視山送？元者具哭想，直往至登放松足丟一青許又色坡！吉道天主昔問支裏加！飽山學卜去物音、婆法生穿室吧肖麼燈欠在掃者消了「孝造假吧口在候申兆風」占事師幾聲！人跑學菜士歡。定邊燈國詞樹、在喜反立松牛寸字子背，子抄幸說二呢根象房。飛起棵聲拍英唱個耳欠。貓兩喝上地說山道給什！壯女年完背那里安昔東出只，包澡朋今耍讀爬「旁跑書穴金讀胡」。一扒兒的記同安入和少借步：以弟造北山時喜占急！國助登喜足玉由許且母國拍再了兆師食打發甲。實浪皮故司幼紅三刀念故自呢要瓜七園，飯助才：鳥你民竹者助手事青早娘九前者回朋來央但畫「朵斤辛連」母邊國完。果工別來吹孝課我，對面做法五叫布告問身寫。只飽字是去已吃王停很出示丁天勿尾相裏？安住雲壯「左走想」尾巴麼頁乍皮方校它即寫，有鴨歡知急合院中綠卜父欠丟。欠乙男以綠喜飛法問者。禾牙河世畫條視。北外助王左蝸美即從几皮還葉示「羽尤前衣旦」食想小兌我竹國您同身點兩個十什怎兩您，平草消太里停封衣。想戊長實虎旦寸頭把四每自別，麻鴨鼻像「紅」冰說又了成。做忍交里至眼比青世丟道停枝，口它鼻抓，新邊合冒再行弟木怕肉晚再成良可親因京親，水跳尾到呀功書視害足虎旁，尾星走固爪雄！每新語'),
    materialize('abcd洋哪五松左婆抱止買聲羽老。鳥蝴母太問原弓半福造夕二喝夕者星邊，過綠成貝常身後京着像草長內！沒可寸學路尺母快，古毛流海多示才想貫豆樹直路吹更，掃時把；眼少只急米百；穴跟雲古，習幼苦進木意和坐消葉又裏媽。午找封荷雲即且國婆得忍美中面別、午什包壯奶一：坡雞馬假包次鳥呢眼化從戶登斗雄你們洋食？午太又條主造行媽，水苗抱事新皮有世實。校孝跟汗了左生，還紅法遠孝斥是現大大定黃姐您唱吹，丁嗎衣新肖反後京幾何止斥外戶：首看七假每，蝶歌光着牠品。十開松節氣幾各法尤各助，田對蝸大足用植合呀幸包足告巾活，再詞爬入同沒一晚重二公半，借以相結洋飽中停問平早對雞我快陽門躲羽，里目地方蛋父笑石具几；犬六員聽石占房抱，黃高玉？停犬今流。你杯馬支葉合，京丟書結哥象瓜、信已給。鼻發對學彩奶室晚話止動跑細弟未豆原黑斤支。光下世斤河乾瓜刀鳥勿石裝兔冒又六十朱？開哪幼跑冬這刃，出的日隻央東，貓登泉合位我；幾升以。國比馬後房從抄忍游澡天要訴吹共雄；多課借昔司進停帶。告寺巾早沒候姊尼記在歌記言畫，下裝止北想聽，錯歡誰支久吉邊石：尺收外只住追天候樹几固雄風哪神弓告停，比節元。吉房拍兩根杯息要直忍牠辛良新音香，者幼就休飽發言丁，員完羊'));

SELECT levenshteinDistance('', '') AS actual, 0 AS expected;
SELECT levenshteinDistance('', 'abc') AS actual, 3 AS expected;
SELECT levenshteinDistance('abc', '') AS actual, 3 AS expected;
SELECT levenshteinDistance('a', 'a') AS actual, 0 AS expected;
SELECT levenshteinDistance('a', 'b') AS actual, 1 AS expected;
SELECT levenshteinDistance('abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk') AS actual, 0 AS expected;
SELECT levenshteinDistance('abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghij') AS actual, 1 AS expected;
SELECT levenshteinDistance('abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijk', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl') AS actual, 1 AS expected;
SELECT levenshteinDistance('abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghij', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijkl') AS actual, 2 AS expected;
SELECT levenshteinDistance('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') AS actual, 0 AS expected;
SELECT levenshteinDistance('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab') AS actual, 1 AS expected;
SELECT levenshteinDistance('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') AS actual, 1 AS expected;
SELECT levenshteinDistance('short', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') AS actual, 84 AS expected;
SELECT levenshteinDistance('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'short') AS actual, 84 AS expected;
SELECT levenshteinDistance('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') AS actual, 0 AS expected;
SELECT levenshteinDistance('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb') AS actual, 100 AS expected;
SELECT levenshteinDistanceUTF8('你好', '你好') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('你好', '您好') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('你好世界', '你好世間') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('你好世界你好世界你好世界你好世界', '你好世界你好世間你好世界你好世界') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('東京', '東京') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('東京', '京都') AS actual, 2 AS expected;
SELECT levenshteinDistanceUTF8('東京都東京都東京都東京都', '東京都東京都京都東京都') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('привет', 'привет') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('привет', 'превет') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('приветмирприветмирприветмир', 'приветмирпреветмирприветмир') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('短', 'abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz') AS actual, 104 AS expected;
SELECT levenshteinDistanceUTF8('abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz', '短') AS actual, 104 AS expected;
SELECT levenshteinDistanceUTF8('漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字', '漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字漢字仮字') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('日本語日本語日本語日本語日本語日本語日本語', '日本語日本語日本語日本語日本語日本語日本語') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('日本語日本語日本語日本語日本語日本語日本語', '日本語日本語日本語日本語日本語日本語英語') AS actual, 2 AS expected;
SELECT levenshteinDistanceUTF8('русскийтекструсскийтекструсскийтекструсскийтекст', 'русскийтекструсскийтекструсскийтекструсскийтекс') AS actual, 1 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 0 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 1 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 39 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 56 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('γειά', 'γειά') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('γειά', 'γεια') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('안녕하세요', '안녕히계세요') AS actual, 2 AS expected;
SELECT levenshteinDistanceUTF8('안녕하세요', '안녕하세요') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('გამარჯობა', 'გამარჯობა') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('გამარჯობა', 'გამმართობა') AS actual, 2 AS expected;
SELECT levenshteinDistanceUTF8('ααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααα', 'ααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααα') AS actual, 2 AS expected;
SELECT levenshteinDistanceUTF8('ααααααααααααααααααααααααααααααααααααααααααααααααααααα', 'ααααααααααααααααααααααααααααααααααααααααααααααααααααα') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가', '가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가') AS actual, 17 AS expected;
SELECT levenshteinDistanceUTF8('가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가', '가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가') AS actual, 2 AS expected;
SELECT levenshteinDistanceUTF8('ააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააааა', 'აააააააააააააააააააააააააააააააააააააააააააააააააააა') AS actual, 11 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 0 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 0 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 0 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 35 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 1 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 1 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAABBBBBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 5 AS expected;
SELECT levenshteinDistance('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'AAAAAAAAAAXXXXXAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA') AS actual, 26 AS expected;
SELECT levenshteinDistanceUTF8('ααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααα', 'ααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααα') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('αααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααα', 'ααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααα') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('αααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααα', 'ααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααα') AS actual, 35 AS expected;
SELECT levenshteinDistanceUTF8('αααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααα', 'ααααααααααααααααααααααααααααααααααααααααβααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααααα') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가', '가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가', '가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가가') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('ააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააა', 'ააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააა') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('ააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააა', 'აააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააა') AS actual, 37 AS expected;
SELECT levenshteinDistanceUTF8('ააააააააააააააააააააააააააააააააბბბაააააააააააააააააააააააააააააააა', 'ააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააააა') AS actual, 4 AS expected;
SELECT levenshteinDistanceUTF8('γειά', 'γειά') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('γειά', 'γεια') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('γειά', 'γειά!') AS actual, 1 AS expected;
SELECT levenshteinDistanceUTF8('안녕하세요', '안녕히계세요') AS actual, 2 AS expected;
SELECT levenshteinDistanceUTF8('안녕하세요', '안녕하세요') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('გამარჯობა', 'გამარჯობა') AS actual, 0 AS expected;
SELECT levenshteinDistanceUTF8('გამარჯობა', 'გამმართობა') AS actual, 2 AS expected;
SELECT levenshteinDistance('ficS8NMdw6ZJ1h9GgUFVFDMjicfthpOu', '9s8NMDw6ZJ1h9GgUFVFDMjicfthpOu') AS actual, 5 AS expected;
SELECT levenshteinDistance('rX44ZgBvRKBlqczckOdm2qLiiSnR53I7d33i6YV4f82UkmQpkRf6tqwLneGxR71Fdasda', 'z2fBSYkJoQhXleCHAhArI7Dk5I6fdznL5pxgtPRjgod6ij3VCzb7h7kZDKxGagaw') AS actual, 65 AS expected;
SELECT levenshteinDistance('Zwx0QyNE2AfZBlLE5oel5LvXaNryAXXqtuuBfUGbu6k5NCXMqLoUYcDHHQ5Ilg8NHbc0FAPImhB6yRIWl18ujCrVjDRqiu7OEzPla1uDkybog1NuIhpuTqTFcXzmLWCXAH2uSTwF1pkMOzeeM586H5BrmMhQsPDf', 'ficS8NMdw6ZJ1h9GgUFVFDMjicfthpOurX44ZgBvRKBlqczckOdm2qLiiSnR53I7d33i6YV4f82UkmQpkRf6tqwLneGxR71FZwx0QyNE2AfZBlLE5oel5LvXaNryAXXqtuuBfUGbu6k5NCXMqLoUYcDHHQ5Ilg8NHbc0FAPImhB6yRIWl18ujCrVjDRqiu7OEzPla1uDkybog1NuIhpuTqTFcXzmLWCX') AS actual, 128 AS expected;
SELECT levenshteinDistanceUTF8('詳で恥総げちうづ住池高そもぽょ宗2合ルヨ変板ゅ果治モコ場午っ任球ょじすり相嗅アトナ供政祥ゃイ万科うが秋フが女消ノワロキ見風ヤヒシス配供は否挑ワレク銀出ハヘイヤ治残ヌ', '道スミチ上天よラルべ健市ホネ現提ど場池ッ分異ヌヨ傷南イナ散一ルクょ河9観ウエリ現盤ラフ作強マ同張え。6木ミセソ作止スフ野写で必般イ欠誕温スソ去毎ク建身っつんド度粉ご前19調まひ後博たを携約禁72報日聞市ヒヤハホ経北傘をぞ。観ツス手建ぐか察禁ヌホヲシ育') AS actual, 120 AS expected;
SELECT levenshteinDistanceUTF8('半テ絶力へじづひ情日ヲフオマ型読9緊覧だづや田立だリ九高わす二申むドい生著るべや断彰ユカツ複95済モナス稿国ネサナユ近無メエ飯承ぱ項天法ら抜乾凍刷くゃイあ。公当記てあ茂全ー間仙サエ藤96送域エチキル資属企済7開フリセラ高5数なル名惑もせむ春午く済並めぜれ数なル名惑もせむ春午く済並めぜ', '話ゅへ成写ミヨ異読サ週製事ぼげ成権ク公地ウコトヘ録物内ンで手制ぜ江務たほ沼部よイとげ所46入ぽ閉載ハウ強集ヘ立問えろ後応披ぜやまよ裁捕藤やこも。京チ食相つてふば予真りぞ熱4一発ヤヒリ月能まぽどお曜主ずンぴ載告キヱ界未ヨセラ意古林ぐめ勝禁ヘヱナワ際無えくちゃ話稿けび文担達リ。公全ツハネレ失16変え軽将ケムトリ量4紙ミカ説海ちレろし年邦健ルチタ養未8既杉乾凍つンろめ。問えろ後応披ぜやまよJapanese Lorem Ipsum is based') AS actual, 215 AS expected;

DROP TABLE t;
