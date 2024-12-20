---
sidebar_label: ローカルファイルを挿入
sidebar_position: 2
---

# ローカルファイルを挿入

`clickhouse-client` を使用してローカルファイルを ClickHouse サービスにストリームすることができます。これにより、多くの強力で便利な ClickHouse 関数を使用してデータを前処理することができます。例を見てみましょう...

1. `comments.tsv`という名前のTSVファイルがあり、いくつかのHacker Newsのコメントが含まれていて、ヘッダー行にはカラム名が含まれています。このデータを挿入する際には、[入力フォーマット](/docs/ja/interfaces/formats)を指定する必要があります。この場合は `TabSeparatedWithNames` です。

```text
id	type	author	timestamp	comment	children
19464423	comment	adrianmonk	2019-03-22 16:58:19	"It&#x27;s an apples and oranges comparison in the first place. There are security expenses related to prison populations. You need staff, facilities, equipment, etc. to manage prisoners behavior (prevent fights, etc.) and keep them from escaping. The two things have a different mission, so of course they&#x27;re going to have different costs.<p>It&#x27;s like saying a refrigerator is more expensive than a microwave. It doesn&#x27;t mean anything because they do different things."	[]
19464461	comment	sneakernets	2019-03-22 17:01:10	"Because the science is so solid that it&#x27;s beating a dead horse at this point.<p>But with anti-vaxxers, It&#x27;s like telling someone the red apple you&#x27;re holding is red, yet they insist that it&#x27;s green. You can&#x27;t argue &quot;the merits&quot; with people like this."	[19464582]
19465288	comment	derefr	2019-03-22 18:15:21	"Because we&#x27;re talking about the backend-deployment+ops-jargon terms &quot;website&quot; and &quot;webapp&quot;, not their general usage. Words can have precise jargon meanings <i>which are different</i> in different disciplines. This is where ops people tend to draw the line: a web<i>site</i> is something you can deploy to e.g. an S3 bucket and it&#x27;ll be fully functional, with no other dependencies that you have to maintain for it. A <i>webapp</i> is something that <i>does</i> have such dependencies that you need to set up and maintain—e.g. a database layer.<p>But even ignoring that, I also define the terms this way because of the prefix &quot;web.&quot; A webapp isn&#x27;t &quot;an app on the web&quot;, but rather &quot;an app powered by the web.&quot; An entirely-offline JavaScript SPA that is just <i>served over</i> the web, <i>isn&#x27;t</i> a web-app. It&#x27;s just a program that runs in a browser, just like a Flash or ActiveX or Java applet is a program that runs in a browser. (Is a Flash game a &quot;web game&quot;? It&#x27;s usually considered a <i>browser game</i>, but that&#x27;s not the same thing.)<p>We already have a term for the thing that {Flash, ActiveX, Java} applets are: apps. Offline JavaScript SPAs are just apps too. We don&#x27;t need to add the prefix &quot;web&quot;; it&#x27;s meaningless here. In any of those cases, if you took the exact same program, and slammed it into an Electron wrapper instead of into a domain-fronted S3 bucket, it would clearly not be a &quot;web app&quot; in any sense. Your SPA would just be &quot;a JavaScript <i>app</i> that uses a browser DOM as its graphics toolkit.&quot; Well, that&#x27;s just as true before you put it in the Electron wrapper.<p>So &quot;web app&quot;, then, has a specific meaning, above and beyond &quot;app.&quot; You need something extra. That something extra is a backend, which your browser—driven by the app&#x27;s logic—interacts with <i>over the web</i>. That&#x27;s what makes an app &quot;a web app.&quot; (This definition intentionally encompasses both server-rendered dynamic HTML, and client-rendered JavaScript SPA apps. You don&#x27;t need a frontend <i>app</i>; you just need a <i>web backend</i> that something is interacting with. That something can be the browser directly, by clicking links and submitting forms; or it can be a JavaScript frontend, using AJAX.)<p>A &quot;web site&quot;, then, is a &quot;web app&quot; without the &quot;app&quot; part. If it&#x27;s clear in the above definition what an &quot;app&quot; is, and what a &quot;web app&quot; is, then you can subtract one from the other to derive a definition of a &quot;web not-app.&quot; That&#x27;s a website: something powered by a web backend, which does not do any app things. If we decide that &quot;app things&quot; are basically &quot;storing state&quot;, then a &quot;site&quot; is an &quot;app&quot; with no persistent state.<p>And since the definition of &quot;web&quot; here is about a backend, then the difference between a &quot;web app&quot; and a &quot;web site&quot; (a web not-app) is probably defined by the properties of the backend. So the difference about the ability of the web backend to store state. So a &quot;web site&quot; is a &quot;web app&quot; where the backend does no app things—i.e., stores no state."	[]
19465534	comment	bduerst	2019-03-22 18:36:40	"Apple included: <a href=""https:&#x2F;&#x2F;www.theguardian.com&#x2F;commentisfree&#x2F;2018&#x2F;mar&#x2F;04&#x2F;apple-users-icloud-services-personal-data-china-cybersecurity-law-privacy"" rel=""nofollow"">https:&#x2F;&#x2F;www.theguardian.com&#x2F;commentisfree&#x2F;2018&#x2F;mar&#x2F;04&#x2F;apple-...</a>"	[]
19466269	comment	CalChris	2019-03-22 19:55:13	"&gt; It has the same A12 CPU ... with 3 GB of RAM on the <i>system-on-a-chip</i><p>Actually that&#x27;s <i>package-on-package</i>. The LPDDR4X DRAM is glued (well, reflow soldered) to the back of the A12 Bionic.<p><a href=""https:&#x2F;&#x2F;www.techinsights.com&#x2F;about-techinsights&#x2F;overview&#x2F;blog&#x2F;apple-iphone-xs-teardown&#x2F;"" rel=""nofollow"">https:&#x2F;&#x2F;www.techinsights.com&#x2F;about-techinsights&#x2F;overview&#x2F;blo...</a><p><a href=""https:&#x2F;&#x2F;en.wikipedia.org&#x2F;wiki&#x2F;Package_on_package"" rel=""nofollow"">https:&#x2F;&#x2F;en.wikipedia.org&#x2F;wiki&#x2F;Package_on_package</a>"	[19468341]
19466980	comment	onetimemanytime	2019-03-22 21:07:25	"&gt;&gt;<i>The insanity, here, is that you can&#x27;t take the land the motorhome is on and build a studio on it.</i><p>apple and oranges. The permit to built the studio makes that building legit, kinda forever. A motor home, they can chase out with a new law, or just by enforcing existing laws."	[]
19467048	comment	karambahh	2019-03-22 21:15:41	"I think you&#x27;re comparing apples to oranges here.<p>If you reclaim a parking space for another use (such as building accommodation for families or an animal shelter), you&#x27;re not depriving the car of anything, it&#x27;s an expensive, large piece of metal and is not sentient.<p>Next, you&#x27;ll say that you&#x27;re depriving car owners from the practicality of parking their vehicles anywhere they like. I&#x27;m perfectly fine with depriving car owners from this convenience to allow a human being to have a roof over their head. (speaking from direct experience as I&#x27;ve just minutes ago had to park my car 1km away from home because the city is currently building housing and has restricted parking space nearby)<p>Then, some might argue that one should be ashamed of helping animals while humans are suffering. That&#x27;s the exact same train of thought with «we can&#x27;t allow more migrants in, we have to take care of our &quot;own&quot; homeless people».<p>This is a false dichotomy. Western societies inequalities are growing larger and larger. Me trying to do my part is insignificant. Me donating to human or animal causes is a small dent into the mountains of inequalities we live on top of. Us collectively, we do make a difference, by donating, voting and generally keeping our eyes open about the world we live in...<p>Finally, an entirely anecdotal pov: I&#x27;ve witnessed several times extremely poor people going out of their ways to show solidarity to animals or humans. I&#x27;ve also witnessed an awful lot of extremely wealthy individuals complaining about the poor inconveniencing them by just being there, whose wealth was a direct consequences of their ancestors exploiting whose very same poor people."	[19467512]
```

2. Hacker News データ用のテーブルを作成しましょう。

```sql
CREATE TABLE hackernews (
    id UInt32,
    type String,
    author String,
    timestamp DateTime,
    comment String,
    children Array(UInt32),
    tokens Array(String)
)
ENGINE = MergeTree
ORDER BY toYYYYMMDD(timestamp)
```

3. `author` カラムを小文字に変換したい場合、これは [`lower` 関数](/docs/ja/sql-reference/functions/string-functions/#lower-lcase)を使うことで簡単にできます。また、`comment` 文字列をトークンに分割し、その結果を `tokens` カラムに格納したい場合、[`extractAll` 関数](/docs/ja/sql-reference/functions/string-search-functions/#extractallhaystack-pattern)を使うことで実現できます。これらはすべて `clickhouse-client` コマンドで行います。`comments.tsv` ファイルは `<` 演算子を使って `clickhouse-client` にパイプで渡されています。

```bash
clickhouse-client \
    --host avw5r4qs3y.us-east-2.aws.clickhouse.cloud \
    --secure \
    --port 9440 \
    --password Myp@ssw0rd \
    --query "
    INSERT INTO hackernews
    SELECT
        id,
	   	type,
   		lower(author),
		timestamp,
		comment,
		children,
		extractAll(comment, '\\w+') as tokens
    FROM input('id UInt32, type String, author String, timestamp DateTime, comment String, children Array(UInt32)')
    FORMAT TabSeparatedWithNames
" < comments.tsv
```

:::note
`input` 関数は、データが `hackernews` テーブルに挿入される際に変換を行うために便利です。`input` の引数は、受け取る生データのフォーマットを示し、他の多くのテーブル関数でもこれが登場します（受信データのスキーマを指定します）。
:::

4. これで完了です！ データが ClickHouse にアップロードされました。

```sql
SELECT *
FROM hackernews
LIMIT 7
```

結果は以下の通りです。

```response

│  488 │ comment │ mynameishere │ 2007-02-22 14:48:18 │ "It's too bad. Javascript-in-the-browser and Ajax are both nasty hacks that force programmers to do all sorts of shameful things. And the result is--wanky html tricks. Java, for its faults, is fairly clean when run in the applet environment. It has every superiority over JITBAJAX, except for install issues and a chunky load process. Yahoo games seems like just about the only applet success story. Of course, back in the day, non-trivial Applets tended to be too large for the dial-up accounts people had. At least that is changed." │ [454927] │ ['It','s','too','bad','Javascript','in','the','browser','and','Ajax','are','both','nasty','hacks','that','force','programmers','to','do','all','sorts','of','shameful','things','And','the','result','is','wanky','html','tricks','Java','for','its','faults','is','fairly','clean','when','run','in','the','applet','environment','It','has','every','superiority','over','JITBAJAX','except','for','install','issues','and','a','chunky','load','process','Yahoo','games','seems','like','just','about','the','only','applet','success','story','Of','course','back','in','the','day','non','trivial','Applets','tended','to','be','too','large','for','the','dial','up','accounts','people','had','At','least','that','is','changed'] │
│  575 │ comment │ leoc         │ 2007-02-23 00:09:49 │ "I can't find the reference now, but I *think* I've just read something suggesting that the install process for an Apollo applet will involve an &#34;install-this-application?&#34; confirmation dialog followed by a download of 30 seconds or so. If so then Apollo's less promising than I hoped. That kind of install may be low-friction by desktop-app standards but it doesn't compare to the ease of starting a browser-based AJAX or Flash application. (Consider how easy it is to use maps.google.com for the first time.)<p>Surely it will at least be that Apollo applications will run untrusted by default, and that an already-installed app will start automatically whenever you take your browser to the URL you downloaded it from?" │ [455071] │ ['I','can','t','find','the','reference','now','but','I','think','I','ve','just','read','something','suggesting','that','the','install','process','for','an','Apollo','applet','will','involve','an','34','install','this','application','34','confirmation','dialog','followed','by','a','download','of','30','seconds','or','so','If','so','then','Apollo','s','less','promising','than','I','hoped','That','kind','of','install','may','be','low','friction','by','desktop','app','standards','but','it','doesn','t','compare','to','the','ease','of','starting','a','browser','based','AJAX','or','Flash','application','Consider','how','easy','it','is','to','use','maps','google','com','for','the','first','time','p','Surely','it','will','at','least','be','that','Apollo','applications','will','run','untrusted','by','default','and','that','an','already','installed','app','will','start','automatically','whenever','you','take','your','browser','to','the','URL','you','downloaded','it','from'] │
│ 3110 │ comment │ davidw       │ 2007-03-09 09:19:58 │ "I'm very curious about this tsumobi thing, as it's basically exactly what Hecl is ( http://www.hecl.org ).  I'd sort of abbandoned it as an idea for making any money with directly, though, figuring the advantage was just to be able to develop applications a lot faster.  I was able to prototype ShopList ( http://shoplist.dedasys.com ) in a few minutes with it, for example.<p>Edit: BTW, I'd certainly be interested in chatting with the Tsumobi folks.  It's a good idea - perhaps there are elements in common that can be reused from/added to Hecl, which is open source under a very liberal license, meaning you can take it and include it even in 'commercial' apps.<p>I really think that the 'common' bits in a space like that have to be either free or open source (think about browsers, html, javascript, java applets, etc...), and that that's not where the money is." │ [3147]   │ ['I','m','very','curious','about','this','tsumobi','thing','as','it','s','basically','exactly','what','Hecl','is','http','www','hecl','org','I','d','sort','of','abbandoned','it','as','an','idea','for','making','any','money','with','directly','though','figuring','the','advantage','was','just','to','be','able','to','develop','applications','a','lot','faster','I','was','able','to','prototype','ShopList','http','shoplist','dedasys','com','in','a','few','minutes','with','it','for','example','p','Edit','BTW','I','d','certainly','be','interested','in','chatting','with','the','Tsumobi','folks','It','s','a','good','idea','perhaps','there','are','elements','in','common','that','can','be','reused','from','added','to','Hecl','which','is','open','source','under','a','very','liberal','license','meaning','you','can','take','it','and','include','it','even','in','commercial','apps','p','I','really','think','that','the','common','bits','in','a','space','like','that','have','to','be','either','free','or','open','source','think','about','browsers','html','javascript','java','applets','etc','and','that','that','s','not','where','the','money','is'] │
│ 4016 │ comment │ mynameishere │ 2007-03-13 22:56:53 │ "http://www.tigerdirect.com/applications/SearchTools/item-details.asp?EdpNo=2853515&CatId=2511<p>Versus<p>http://store.apple.com/1-800-MY-APPLE/WebObjects/AppleStore?family=MacBookPro<p>These are comparable systems, but the Apple has, as I said, roughly an 800 dollar premium. Actually, the cheapest macbook pro costs the same as the high-end Toshiba. If you make good money, it's not a big deal. But when the girl in the coffeehouse asks me what kind of computer she should get to go along with her minimum wage, I'm basically scum to recommend an Apple." │ []       │ ['http','www','tigerdirect','com','applications','SearchTools','item','details','asp','EdpNo','2853515','CatId','2511','p','Versus','p','http','store','apple','com','1','800','MY','APPLE','WebObjects','AppleStore','family','MacBookPro','p','These','are','comparable','systems','but','the','Apple','has','as','I','said','roughly','an','800','dollar','premium','Actually','the','cheapest','macbook','pro','costs','the','same','as','the','high','end','Toshiba','If','you','make','good','money','it','s','not','a','big','deal','But','when','the','girl','in','the','coffeehouse','asks','me','what','kind','of','computer','she','should','get','to','go','along','with','her','minimum','wage','I','m','basically','scum','to','recommend','an','Apple'] │
│ 4568 │ comment │ jwecker      │ 2007-03-16 13:08:04 │ I know the feeling.  The same feeling I had back when people were still writing java applets.  Maybe a normal user doesn't feel it- maybe it's the programmer in us knowing that there's a big layer running between me and the browser...                 │ []       │ ['I','know','the','feeling','The','same','feeling','I','had','back','when','people','were','still','writing','java','applets','Maybe','a','normal','user','doesn','t','feel','it','maybe','it','s','the','programmer','in','us','knowing','that','there','s','a','big','layer','running','between','me','and','the','browser'] │
│ 4900 │ comment │ lupin_sansei │ 2007-03-19 00:26:30 │ "The essence of Ajax is getting Javascript to communicate with the server without reloading the page. Although XmlHttpRequest is most convenient, there were other methods of doing this before XmlHttpRequest such as <p>- loading a 1 pixel image and sending data in the image's cookie<p>- loading server data through a tiny frame which contained XML or javascipt data<p>- Using a java applet to fetch the data on behalf of javascript" │ []       │ ['The','essence','of','Ajax','is','getting','Javascript','to','communicate','with','the','server','without','reloading','the','page','Although','XmlHttpRequest','is','most','convenient','there','were','other','methods','of','doing','this','before','XmlHttpRequest','such','as','p','loading','a','1','pixel','image','and','sending','data','in','the','image','s','cookie','p','loading','server','data','through','a','tiny','frame','which','contained','XML','or','javascipt','data','p','Using','a','java','applet','to','fetch','the','data','on','behalf','of','javascript'] │
│ 5102 │ comment │ staunch      │ 2007-03-20 02:42:47 │ "Well this is exactly the kind of thing that isn't very obvious. It sounds like once you're wealthy there's a new set of rules you have to live by. It's a shame everyone has had to re-learn these things for themselves because a few bad apples can control their jealousy.<p>Very good to hear it's somewhere in your essay queue though. I'll try not to get rich before you write it, so I have some idea of what to expect :-)" │ []       │ ['Well','this','is','exactly','the','kind','of','thing','that','isn','t','very','obvious','It','sounds','like','once','you','re','wealthy','there','s','a','new','set','of','rules','you','have','to','live','by','It','s','a','shame','everyone','has','had','to','re','learn','these','things','for','themselves','because','a','few','bad','apples','can','control','their','jealousy','p','Very','good','to','hear','it','s','somewhere','in','your','essay','queue','though','I','ll','try','not','to','get','rich','before','you','write','it','so','I','have','some','idea','of','what','to','expect'] │

```

5. もう一つのオプションとして、`cat` のようなツールを使用して `clickhouse-client` へファイルをストリームする方法があります。以下のコマンドは、`<` 演算子を使用した場合と同じ結果になります。

```bash
cat comments.tsv | clickhouse-client \
    --host avw5r4qs3y.us-east-2.aws.clickhouse.cloud \
    --secure \
    --port 9440 \
    --password Myp@ssw0rd \
    --query "
    INSERT INTO hackernews
    SELECT
        id,
	   	type,
   		lower(author),
		timestamp,
		comment,
		children,
		extractAll(comment, '\\w+') as tokens
    FROM input('id UInt32, type String, author String, timestamp DateTime, comment String, children Array(UInt32)')
    FORMAT TabSeparatedWithNames
"
```

`clickhouse-client` のインストールに関する詳細は、[`clickhouse-client`のドキュメントページ](/docs/ja/integrations/sql-clients/clickhouse-client-local)をご覧ください。

## 関連コンテンツ

- ブログ: [Getting Data Into ClickHouse - Part 1](https://clickhouse.com/blog/getting-data-into-clickhouse-part-1)
- ブログ: [Exploring massive, real-world data sets: 100+ Years of Weather Records in ClickHouse](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
