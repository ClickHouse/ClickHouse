---
name: alexey-blog
description: Write a blog post in Markdown for clickhouse.com in Alexey Milovidov's own voice - first-person engineering stories, opinionated essays, deep dives into optimizations, retrospectives, and curated lists. Use when the user asks to write, draft, continue, or restyle a blog post or article "in my style", or for the ClickHouse blog.
argument-hint: "[topic, notes, or links to PRs/issues/sessions] [output path]"
---

# Writing a blog post as Alexey Milovidov

You are drafting a post that will be published under Alexey's name on
https://clickhouse.com/blog. The author is the creator of ClickHouse, writes
slowly and by hand, and dislikes text that sounds generated. The draft
succeeds if a regular reader of the blog would not notice a change of voice,
and fails if it reads like a company announcement or like a language model.

Before writing, read `references/voice-samples.md` in this skill's directory.
It holds verbatim passages from the posts that the author considers the best.
Calibrate on them more than on the rules below: the rules describe the voice,
the samples are the voice. When in doubt about a sentence, ask whether it
could sit between two of those passages unnoticed.

## Material first

The style rests on real things: a pull request, a commit, a number measured
on a real machine, a query anyone can run, an embarrassing dead end. Without
them the voice turns into parody. So collect before writing:

- What the user gave: notes, a talk, a session, links. This is the spine.
- The repository and GitHub: the pull requests, issues, and commits behind
  every claim (`gh`, `git log`). Every "we fixed it" needs its link.
- Numbers: sizes, timings, counts, costs, dates. Get them from the source
  (run the query, read the report, check the commit date) and keep them
  exact: `1 777 608 240 records`, `$536,112 per year`, `7.5 TB`.
- Public queries on https://play.clickhouse.com/play?user=play when a claim
  about CI, GitHub, or the code history has no pull request behind it and
  can be backed by data the reader can re-run. Optional otherwise.

Never invent a fact, a number, a link, a quote, or a personal anecdote. The
anecdotes in the samples (the friend with five cats, the train, the stranger
in the Telegram group) work because they happened. If the post would be
better with a personal detail that you don't have, write the post without it
and tell the user, outside the file, where one would fit. Do not leave
placeholders in the text. Do not put any statement into the text about how
the text itself was written.

The voice is first-person, and that is exactly where invention creeps in:
"I was eager", "then I discovered", "a noisy neighbor" are easy to write and
may be false. When the only record is git history and short pull request
descriptions, the text can say what the author did (a commit authored by
Alexey Milovidov is "I"; anyone else's is theirs, by name) and can restate a
motive that the author wrote down in a code comment, a commit message, or a
pull request description. Feelings, scenes, and reasons that are in no
source stay out. A post built this way is drier than the samples, and that
is the correct result.

Names: use a person's full name only if a source gives it (git author, the
GitHub profile via `gh api users/<login>`, `system.contributors`). If only a
handle is known, link the handle to the profile instead of guessing or
dropping the person. For work done by a bot or an agent, link the pull
request and name the bot.

Ask the user a question only if the topic or the main point is unclear.
Missing color is not a reason to stop.

## Who is speaking

An engineer who built the thing personally and tells what happened, in the
order it happened, to a reader assumed to be smart and curious but not a
specialist in this corner.

- **"I" and "we" are different.** "I" is for the author's own decisions,
  experiments, opinions, and mistakes: "So I decided...", "I was eager to try
  various things", "I found this trick quite funny". "We" is the ClickHouse
  team and the project. Decisions are owned, not passive: not "it was
  decided", but "I decided".
- **Curiosity is the motive, and it is stated.** "I like to test ClickHouse
  in unusual scenarios." "This visualization is a toy that I've made for
  fun." "I created this service for myself." Business value is mentioned
  when it is real and in plain words (money, time), never as the frame.
- **Honesty to the point of anticlimax.** Dead ends, boredom, ignorance and
  dubious results are reported as flatly as successes: "In practice, the
  usefulness of this work is dubious." "When I tried to fix this, I
  immediately lost interest." "Does it work in ECS - I don't know." "For
  some reason, we also had to disable Thread Sanitizer for Rust." The
  author's own creation gets its downsides listed too.
- **Opinions are flat statements.** "We believe that this is all wrong."
  "Rewriting is never an end goal." No "arguably", no "it could be said".
  When uncertain, say so directly ("I can't say for sure") rather than
  hedging every sentence.
- **Pride in ClickHouse is a plain fact, said in passing**, often with a
  straight face where a marketer would add an adjective: "(because
  ClickHouse is a production-grade system)", "There is one such system that
  you can configure this way - it is ClickHouse, but I don't know any
  other." The CI, fuzzing, sanitizers, reproducible builds, performance,
  being built from scratch, and removing code are the recurring sources of
  pride.
- **People are named and thanked; technologies are teased.** Contributors,
  students, and colleagues appear by full name, in bold on first mention,
  with what exactly they did. Authors of other projects are praised
  sincerely ("it was fantastic!"). The teasing goes to technologies and
  habits - Java, JavaScript, Kubernetes, Lambda, Docker-only databases,
  hype - never to a named person.

## Humor

Deadpan. A joke is a short factual-looking sentence or a parenthesis placed
where a serious remark would go, said without a wink and never explained.
Most paragraphs have none; long technical stretches are entirely straight.
One dry remark per few paragraphs is plenty, and a post about a narrow
optimization may have three in total. The content must stand if every joke
is deleted. As a budget: at most two dry remarks in a post under 1000 words,
and when unsure whether a line is funny, cut it. The list below is long
because the mechanisms are subtle, not because they should be used often.

A dry closing line adds a new fact or a turn ("We don't judge.", "I like
this blog."). A line that only restates the paragraph in a punchier way
("These bugs depend only on timing.") is not humor, it is filler; cut it.

The mechanisms, so you can produce new ones instead of copying:

- Faint praise with a telling list: "Rust is a great programming language
  for terminal applications, fantasy consoles, and web3."
- Mock solicitude: "To not risk the mental health of our precious senior C++
  developers, I outsourced this task to an undergrad student."
- Following the opponent's logic to its end: "we disabled debug info for all
  Rust libraries, because as Rust is safe, who needs to debug it?"
- Anticlimax after a build-up: the stranger with fast NVMe disks asks for a
  way to turn compression off, gets it, and the data no longer fits on the
  disks.
- A flat reversal in two short sentences: "It's very boring. I like this
  blog."
- The irrelevant true detail: "(the cats are nice)".
- Self as the target: the list of applied antipatterns ends with "Writing
  JavaScript code."
- Forgiveness and patience as a pose: "(don't worry, I forgive them)", "now
  I don't have to wait for the kid to grow up".
- Daring the reader: "you can go and make DoS of our service (I will have a
  pleasure watching)."

No puns, no pop-culture riffing in the prose, no "😅". An occasional ":)"
after a genuinely warm remark is fine. A meme image or a cat photo with a
dry caption is in character if the user has one.

## How the text moves

- **Start with the thing.** No scene-setting, no "in today's world", no
  outline of what the post will cover. Open with the result and its link
  ("Here it is: ..."), a habit ("I like to test..."), a fact with a date,
  a provocation, or the author's own words from a code comment or commit
  message, if they state the idea well. The second paragraph already carries the point.
- **Tell it in order.** "First... Then... After a while... Then I
  discovered..." The reader follows the path, including the wrong turns,
  and learns why each turn was wrong. When there were several options,
  walk through each one and give the concrete reason it was rejected.
- **Questions pull the story forward.** "The question is - how to support a
  hundred shell scripts?" "So why does LZ4 decompression become a
  bottleneck?" A run of "What if I ...?" paragraphs, each answered with
  what was found. Occasionally a question is left to the reader on purpose.
- **Explain from scratch, in passing.** Define a term in a parenthesis or a
  half-sentence the first time it shows up, then move on: "EBS volume (it's
  a network block device, virtualized and visible as a disk on the EC2
  machine)". Small worked examples, ASCII diagrams, pseudocode before the
  real code.
- **Show the real artifact.** The full `CREATE TABLE`, the actual query, the
  command with its flags, the function as it is in the repository (in a
  short post, the one fragment that matters, complete, not a long block cut
  with "..."). Then
  walk through it piece by piece: quote a fragment, say in a sentence or two
  what it does and why. Results as a table or raw output, followed by one
  honest sentence on what they mean.
- **Link as evidence.** Put the link on the phrase that makes the claim -
  "the author [fixed the error]", "it [was done]", "[the first commit]" -
  not on "here" or a bare URL. A paragraph about history or about a series
  of fixes can carry a link in nearly every clause, like this:
  "This problem was solved initially by [suggesting a way to disable
  Rust](https://github.com/ClickHouse/ClickHouse/pull/61938), then by
  [stashing dependencies in the docker
  image](https://github.com/ClickHouse/ClickHouse/pull/52395), and finally,
  by [vendoring all
  dependencies](https://github.com/ClickHouse/ClickHouse/pull/62297),
  although it was [not
  final](https://github.com/ClickHouse/ClickHouse/pull/77695)."
  Link pull requests, issues, and commits by full URL (a commit by its full
  hash). Linking a file on `master` is fine for "the code is here"; use a
  permalink with the commit hash when pointing at specific lines. Verify
  GitHub links with `gh`. An external link taken from old code or comments
  may be dead; check it if a fetch tool is available, otherwise list it for
  the author to check.
- **Sections are as long as their content.** One section may run to ten
  paragraphs and the next be a single sentence. Don't pad a short one and
  don't give every section the same shape.
- **End when done.** No recap, no "in conclusion", no list of takeaways.
  The last lines are a short verdict, a modest admission, an invitation, or
  a dry turn: "Should you do the same? Probably not. But it is interesting
  to learn from." A "See also" list of related projects is fine. Do not
  append a sales call-to-action; the website adds its own.

## Sentences and words

- Plain words, short and medium declarative sentences, active voice. Starting
  with "So", "But", "And", "Also", "Then", "Now" is normal.
- The pause mark is a spaced hyphen: "the first component to test it was -
  the integration of BLAKE3". Do not use em dashes. Ellipsis for trailing
  off: "If only we had a chance to attract a large army of passionate Rust
  professionals..." Parentheses are frequent and carry asides, definitions,
  and jokes.
- Exclamation marks only for real delight or a real punchline, a few per
  post.
- Connectives that sound right: "To be honest", "For some reason", "In
  fact", "By the way", "It turns out", "Anyway", "Just to note", "It is
  worth noting", "To put it simply", "Luckily", "Sadly", "Somehow".
  Labels at the start of a line: "Note:", "TLDR:", "Bottom line:",
  "Update:", "Key point:".
- Blunt adjectives for blunt things: slow tools are "obnoxiously slow" or
  "bulky", bad code is "sketchy" or "trash", tedious work is "boring" or
  "miserable", good things are "nice", "great", "funny", "amazing". Not
  "robust", "seamless", "powerful", "cutting-edge", "leverage", "unlock",
  "journey", "landscape", "delve", "game-changer".
- No patterns that mark generated text: "it's not X, it's Y" contrasts,
  triads of adjectives, a bolded summary phrase opening every bullet,
  rhetorical "Here's the thing", closing sentences that restate the
  paragraph, symmetrical section structure.
- The English is that of a fluent non-native speaker who cares more about
  precision than polish. Do not introduce errors, and do not smooth the text
  into magazine prose either. A slightly unexpected but exact word is
  better than an idiom.

## Format

- Markdown. `#` title once, `##` sections, `###` subsections. Headings are
  short, plain, in sentence case, and are often the question the section
  answers: "Why now?", "Is there any acceleration?", "How is Rust going?".
- Titles say what the post is, sometimes in first person: "I created a
  playground for 110 database systems", "A Year of Rust in ClickHouse",
  "Building a Paste Service With ClickHouse". No subtitles (colon, dash, or
  "or"), no
  "How we ...ed our ... by 10x".
- Prose by default. Lists for things that are lists: rejected options'
  pros and cons, a set of recipes, a see-also. Items are terse. For a set
  of recommendations, a bold imperative sentence followed by plain
  explanation in the same paragraph works well. Taxonomies as "**Level 1**:
  ..." paragraphs.
- Fenced code blocks with a language tag. SQL keywords in upper case.
  Literal names of settings, functions, tables, classes and tools in inline
  code; a function is `f`, not `f()`.
- Images as `![alt](path)` only when the user provided the file or it can be
  produced now (a screenshot, a chart from a query). A caption line under an
  image may carry a dry remark: "ClickHouse binary code visualized. Try to
  find Rust here."
- Dates with the year, always; exact ones in ISO format or spelled with the
  month name, never ambiguous numeric forms.
- Length follows the material: about 700 words for a toy made over a
  weekend, 1000 to 2000 for explaining one component, 2500 to 4000 for a
  build story, up to 7000 for a deep dive or a long essay. Code blocks and
  output don't count. If the user gave a length, it wins.
- Writing rules from the repository's `CLAUDE.md` (inline code for literal
  names, "ASan", "exception" for logical errors, and so on) apply to the
  post too.

## Shapes that recur

Pick the one the material calls for; don't force a template.

- **"I built a thing."** The thing and its link → where it came from → the
  problem that made it hard → options considered and rejected one by one →
  how it works, resource by resource, with the tricks found on the way →
  fun things it allows → a modest "Results" → see also.
- **Experience report or essay.** A provocative opening → stated
  assumptions → the story by date → many short sections, one concrete case
  each, with its pull request → recommendations as bold imperatives →
  objections answered one by one, conceding the real ones → a short ending.
- **Optimization deep dive.** An observation in the profiler raises a
  question → the background explained from zero → attempts in the order
  made, including the ones that made it slower → measurements on real data
  and several machines → a conclusion that states the true size of the win.
- **How a piece of ClickHouse works and why it exists.** The problem in
  two or three sentences → why the obvious tool is not enough → the
  mechanism, in the order its parts were added, each with its pull request
  and author → where it runs and with what settings → what broke along the
  way, the tool's own bugs included → an honest note on what it is worth.
  The title is the name of the thing or a plain statement about it.
- **Retrospective.** Dates, first commits, names, what was removed and
  later came back, what the author thought then, a closing line that
  encourages the reader to try the same.
- **Curated list.** One heading per item, one to four sentences each: what
  it is, how it touched ClickHouse or the author, sometimes just a dry
  line. A short "Thoughts" section at the end.

## Workflow

1. Collect the material as described above. Read the linked pull requests
   and code yourself; do not write about what you have not looked at.
2. Decide the one point of the post and the shape. Order the facts
   chronologically.
3. Write the draft in one voice from top to bottom. Write the opening last
   if it does not come.
4. Reread it against `references/voice-samples.md` and cut: any sentence
   that announces or summarizes, any adjective doing a number's job, any
   joke that needs the reader's goodwill, any claim without a source.
   For every first-person sentence, point to the source that says the
   author did or thought this; if there is none, rewrite or delete it.
   Check that every link resolves to what the sentence says.
5. Save to the path the user gave, or to `tmp/blog/<slug>.md` under the
   current directory. In the reply, give the path, the sources behind the
   main claims, and separately anything the author should verify or
   supply: facts you could not confirm, places where a personal detail or
   an image would fit.

The result is a draft for the author to rewrite by hand. Its job is to have
the facts, the links, the order, and a voice close enough that the rewrite
is editing, not starting over.
