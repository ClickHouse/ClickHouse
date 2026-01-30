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

DROP TABLE t;
