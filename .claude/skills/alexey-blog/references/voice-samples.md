# Voice samples

Verbatim passages from Alexey Milovidov's posts on https://clickhouse.com/blog
(links in the originals are omitted here; in the posts, most claims are
linked to a pull request, commit, issue, or query; dashes are normalized to
the spaced hyphen that the recent posts use). Read them for rhythm,
stance, and how much is left unsaid. Do not reuse the sentences.

Sources, in the author's order of preference:
`/blog/rust`, `/blog/agentic-coding`, `/blog/open-source-10`,
`/blog/clickbench-playground`, `/blog/tech-blogs`,
`/blog/christmas-gifts-2025`,
`/blog/interactive-visualization-analytics-adsb-flight-data-with-clickhouse`,
`/blog/lz4-compression-in-clickhouse`,
`/blog/five-methods-of-database-obfuscation`,
`/blog/building-a-paste-service-with-clickhouse`,
`/blog/decorating-a-christmas-tree-with-the-help-of-flaky-tests`,
`/blog/evolution-of-data-structures-in-yandexmetrica`.

All of these posts are written by the author and are equally valid as a
reference for the voice.

## Openings

> Here it is: benchmark.clickhouse.com/playground. You can choose any of
> these hundred database systems and run queries. You can create tables and
> databases, insert data, drop tables, etc.

> I like to test ClickHouse in unusual scenarios. Whenever someone (including
> myself) says that ClickHouse is not good for a particular task, I
> immediately go and test it on exactly this task to see how it behaves.
> Sometimes it leads to unusual, and very interesting, outcomes.

> Rust is a great programming language for terminal applications, fantasy
> consoles, and web3. If you do an experiment and say "C++" anywhere on the
> Internet, in a minute someone will chime in and educate you about the
> existence of Rust. Sadly, ClickHouse is written in C++, not in Rust. If
> only we had a chance to attract a large army of passionate Rust
> professionals... So, I decided that we must add Rust to ClickHouse.

> When you run queries in ClickHouse, you might notice that the profiler
> often shows the LZ_decompress_fast function near the top. What is going
> on? This question had us wondering how to choose the best compression
> algorithm.

> I want to avoid this confusion and make it clearer. We use coding agents
> in ClickHouse, and they are a great tool for certain scenarios.

## Telling it in order, with the wrong turns

> At first, I attached a large EBS volume (it's a network block device,
> virtualized and visible as a disk on the EC2 machine), but it was
> obnoxiously slow - to the point I couldn't develop the service. EBS volumes
> in AWS have an option to set up higher IOPS and throughput, but it is
> expensive. So I decided to use a machine with real disks (e.g.,
> `r6id.metal`). But its disk size is only 7.5 TB - not enough for our needs.

> The problem was that it was not enough to fit everything into 7.5 TB. I
> decided to recreate everything using a compressed filesystem. Which Linux
> filesystem supports both reflinks and compression? XFS doesn't support
> compression. The only remaining options are BtrFS and, probably, ZFS.
> Initially, I didn't trust BtrFS because I thought it was some sort of
> parody attempt to reimplement ZFS, but worse. But it supports both
> compression and reflinks, so I didn't have much choice, and I don't worry
> at all about the data in Playground.

> I was really happy at first, when I saw that everything had accelerated by
> such a large percentage. Then I saw that nothing was any faster with other
> files. It was even a little bit slower for some of them.

> Unfortunately, the problem wasn't solved. We performed a few experiments,
> and it just got worse. The only thing left was to sit around doing nothing
> and surf the web randomly since the magic was gone. Luckily, I came across
> a page that explained the algorithm for rendering the death of the main
> character in the game Wolfenstein 3D.

> That's how I was searching for *any* solution that works - any type of
> databases, any libraries, etc. Can we use TokuDB? Colleagues use LMDB,
> maybe it will save us? Let's try Judy Arrays. Someone at lunch told us
> about Hadoop, should we use it? I heard briefly about LZO and QuickLZ in
> the corridor - let's try it.

## Options considered and rejected

> We can host them on AWS EC2 machines. ClickBench uses c6a.4xlarge as the
> most popular machine. It has 32 GB RAM and costs $0.6120 per hour. Keeping
> all systems running will cost around $536,112 per year - that's too
> expensive. I don't want to spend half a million on my little experiment.

> Someone might recommend spot EC2 instances (the instances you buy with a
> lower price, which is decided by the demand, and which can be taken away
> at any time) and use autoscaling groups, but that someone might be an AWS
> consultant.

> I also ruled out Kubernetes. I know some friends who love Kubernetes
> because they are in an abusive relationship with its complexity.

## "What if" runs

> What if we increase the dataset size ten times? How will the result
> change? I found that one of the entrants does not allow large datasets at
> all - they set up the threshold on the dataset size for the non-commercial
> version just to fit our benchmark. We don't judge.

> What if I keep all the systems running and allow you to query them? This
> is the idea of ClickBench Playground.

## Deadpan

> ClickHouse is very portable. For example, people from IBM want to run
> ClickHouse on big-endian s390x systems, so we build it in our CI. My old
> friend, who lives in a garage with five cats wants to run ClickHouse on
> FreeBSD, so we build it for FreeBSD (the cats are nice).

> Also, we disabled debug info for all Rust libraries, because as Rust is
> safe, who needs to debug it?

> Some of my colleagues were Java engineers, so they hooked us on ZooKeeper
> as a coordination system (don't worry, I forgive them)

> But if you set `wait_for_async_insert` to 0, ClickHouse will instantly
> transform itself to MongoDB.

> Here we limit the data size on INSERT. 10 megabytes is quite generous. It
> should be enough to paste a stack trace from a Java application if needed.

> After I told you about this rudimentary anti-fraud, it no longer has any
> effect and you can go and make DoS of our service (I will have a pleasure
> watching).

> The last articles are all about how he compiled a certain version of MySQL
> or PostgreSQL and compared performance. It's very boring. I like this
> blog.

> This is a blog about compilers, linkers, and executable binary formats.
> The amount of information in this blog will make you worry about yourself.

> But then the contributor disappeared somewhere - maybe they had a newborn
> child, I don't know - that's understandable. But now I don't have to wait
> for the kid to grow up, I can ask an agent to finish the pull request.

> (The question is - why does it load 25 MB of JavaScript? If our docs were
> vibe-coded, I would understand that, but they are not vibe-coded. It is
> just how JavaScript is.)

> Don't worry about Sharif, though. He successfully defended his thesis, and
> we happily forgot about the whole thing.

> To be honest, I don't trust any database that requires Docker to run. Good
> databases, like ClickHouse, work fine on any system. But I still wanted to
> support all the other, not-so-good databases.

## Honesty and anticlimax

> In practice, the usefulness of this work is dubious. Yes, LZ4 decompression
> was accelerated on average by 12-20%, and on some datasets, the performance
> more than doubled. But in general, this doesn't have much effect on query
> execution time. It's difficult to find real queries that gain more than a
> couple of percents in speed.

> I don't want to make it sound like this problem was particularly important.
> It was never actually included in planning, and no one had intentions to
> work on it. I hoped that an idea would come up someday, and suddenly I
> would be in a good mood and be able to put everything else off until later.

> There is one problem: in addition to knowing nothing about neural networks
> and machine learning, I am also quite ignorant when it comes to
> cryptography. That leaves just my courage.

> Update: While writing this article I found that the problem is not fixed,
> and it became worse: now some symbols from Rust approach almost a megabyte
> in size. I have to fix it urgently.

> It is the same as the reference implementation but adapted for JavaScript.
> We use modern features like BigInt and TypedArray. I'm not a JS developer,
> so I spend half a day in MDN and Stackoverflow debugging this
> implementation.

> As expected, this doesn't change the performance at all. I just really
> wanted to try optimization for copying 16 bytes at once.

## Opinions, said flat

> The problem is how to deal with flaky tests. Some people suggest
> automatically muting the "annoying" flaky tests. Or adding automatic
> retries in case of failure. We believe that this is all wrong. Instead of
> trying to ignore flaky tests, we do the opposite: we put maximum effort
> into making the tests even more flaky!

> I believe that removing unnecessary code is more important than adding new
> code, and today, removing code is my favorite thing to do.

> Now there is no excuse not to fix small bugs. Don't ask your manager, ask
> an agent!

> Also worth noting that there are plenty of small services offering
> automatic code reviews. In my opinion, if a company can be replicated by a
> single prompt, there is no value in it.

> To put it simply, an SRE can investigate a production problem successfully
> with agents, while a VP will take the wrong hypotheses and fail to solve
> the customer's problem.

> My principle is - never forget. So I continued to remind people about this
> task every week, then every month, then a few times a year, then I
> remained the only person who cared about preventing the original
> (long-time solved) problem.

## Explaining from scratch

> C++ templates are notorious for slowing down compilation, bloating binary
> sizes, and generating very long symbol names (symbol name is the name of a
> piece of code or data in the binary file, used by the linker to bind
> different libraries together). But Rust can be even worse.

> By the way, this type of hash table (with a fixed size and no resolution of
> collisions) is called a "cache table". This name makes sense because, in
> the event of a collision, the cache table simply forgets about the old
> entry.

> This is like a miracle: we take a function that produces random garbage
> based on data, insert it into the Feistel network, and we now have a
> function that produces random garbage based on data, but yet is
> invertible!

> So the swap space of the guest machine is mapped to the page cache of the
> host machine, and as long as the total memory space of the host machine is
> not contended, the guest machine will work as fast as if it had a larger
> memory amount. This makes the memory resource also elastic, like the CPU,
> and I found this trick quite funny.

## Walking through code

> Just in case, we check that the hash calculated on the client side is
> actually a hash of the data. Otherwise someone can connect to ClickHouse
> directly and send arbitrary garbage. In fact they still can. But this
> constraint will constrain the kinds of garbage that we are going to
> receive.

> The only trick is to check request_num to avoid race conditions - the URL
> in the address bar should not point to older results if requests were
> completed out of order. I see this type of race condition in about half of
> the websites, so I always remember about it.

> It copies the first 4 bytes one by one, skips ahead by some magic number,
> copies the next 4 bytes entirely, and moves the cursor to a match using
> another magic number. The author of the code (Yan Collet) somehow forgot
> to leave a comment about what this means. In addition, the variable names
> are confusing.

## Links as evidence (links shown as [bracketed] phrases)

> This problem was solved initially by [suggesting a way to disable Rust],
> then by [stashing dependencies in the docker image], and finally, by
> [vendoring all dependencies], although it was [not final].

> Then [compression was added]. Initially, it was [QuickLZ], but as soon as
> I read [Yann Collet's blog], I replaced it with [LZ4].

> It turns out it is not so easy in Rust. Well, maybe it is easy in Rust,
> and even easier than in C++, but when you [use Rust and C++ together], it
> is [not easy].

## People

> In a few months, thanks to **Denis Bolonin**, we had the first code in
> Rust in ClickHouse.

> In 2012, I had a chance to hire the employee №2 in my team, **Michael
> Kolupaev**, and I have the pleasure to work with him to this day.

> I integrated LZ4 in ClickHouse in 2012, and it was fantastic! I remember
> how I was amazed and eager to see his zhuff algorithm open-sourced, but he
> did even more by releasing zstd in 2015, which I immediately integrated in
> ClickHouse, replacing quicklz. We even found a few bugs in the first 0.x
> versions of zstd :)

> At the Highload++ Siberia conference, an attendee came up to me after my
> presentation and mentioned that for the 8-byte case, you can just use
> multiplication by a specially selected constant (you will also need an
> offset) - this hadn't even occurred to me before!

## Endings

> This service works and I successfully applied the following antipatterns:
>
> - ClickHouse is facing directly to the internet.
> - Replication across the Atlantic Ocean.
> - Two nodes in the RAFT ensemble instead of three.
> - Using an experimental Replicated database engine.
> - Writing JavaScript code.
>
> Should you do the same?
>
> Probably not.
>
> But it is interesting to learn from.

> **Rust is going great!** If your friend is eager to write something in
> Rust, please invite them to ClickHouse, and we guarantee a warm welcome.

> I created this service for myself. I love to collect various database
> systems, production and experimental, popular and obscure, even weird and
> crackpot databases. Now I have a place for the collection. Also, I learned
> about virtualization and operating systems much more than I planned to.

> In any case, the changes were incorporated into master, and the ideas that
> resulted from this research have been applied in other tasks.

> I want this story to also motivate every engineer to try open-sourcing
> their code. In the worst case, nothing will come out of it, but there is a
> chance it will influence generations, as ClickHouse does! Don't worry
> about being ashamed of your code - I just showed you my code from fifteen
> years ago, and it looks kind of funny.

> Sometimes there are rumors that the authors are still alive and continue
> to write somewhere on darknetFacebook, but about therapy, philosophy, or
> parenting. What I really want is for them to continue writing great
> articles about technology!
