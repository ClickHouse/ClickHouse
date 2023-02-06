# Contributing

---
## Bug Reports and Feature Requests
---
If you think you've found a bug or would like to see a feature in one of the [upcoming releases](https://github.com/pocoproject/poco/milestones), file an [issue](https://github.com/pocoproject/poco/issues). Please make sure that your explanations are clear and coherent; do the homework of understanding the problem and searching for existing solutions before posting.

Possible security issues or vulnerabilities can also be reported via email directly to the core team security AT pocoproject.org. The core team will respond to security issues within 24 hours.

If you're in a hurry, the fastest way to have bugs fixed or features added are code contributions. Good code contributions, to be precise; if you want to contribute, read on ...

---


## Code Contributions
---
Before writing any code, please read the

* [C++ Coding Style Guide](http://www.appinf.com/download/CppCodingStyleGuide.pdf) and
* [Git branching model](http://nvie.com/posts/a-successful-git-branching-model/)

[Pull requests](https://help.github.com/articles/using-pull-requests/) are our favorite channel for code contributions and the quickest way to get your fix or feature upstreamed and released. Here's a quick guide.

1. [Fork](https://help.github.com/articles/fork-a-repo/) the [POCO](https://github.com/pocoproject/poco) repo

2. Keep your repo [synced](https://help.github.com/articles/syncing-a-fork/) with the upstream to ensure smooth progress (i.e. that your changes do not interfere or conflict with someone elses's work). Note that the [develop](https://github.com/pocoproject/poco/tree/develop) branch is where most of the ongoing development happens. For new features or libraries, create a new branch with a descriptive camel-cased name. See the mentioned [branching model](http://nvie.com/posts/a-successful-git-branching-model/) for details.

3. Write the code changes; make sure they compile

4. Write the tests (if applicable, and it is in most cases); make sure they pass

5. Test your changes (preferably at least on major platforms - Linux, Windows, Mac)

6. Send [pull request](https://help.github.com/articles/using-pull-requests/) with a descriptive and clear commit message

At this point, it's our turn; if you've done everything well, we may just thank you and merge your request. Otherwise, we may provide some comments or suggestions to steer the contribution in the right direction.

---

## Contributing Mini-FAQ
---
**Q:** Is there any type of contributing license agreement that I have to sign to contribute new features to POCO?

**A:** Currently, you are not required to sign a contributing license agreement. What we require is that you put all your contributions under the [Boost license](https://spdx.org/licenses/BSL-1.0). Also, for contributions that go into existing libraries, you are requested, for reasons of maintaining consistency, to assign the copyright to **"Applied Informatics Software Engineering GmbH and Contributors"**, by putting the corresponding license header in your source file (see the existing source files). If you want to contribute a complete library, you are free to keep the copyright to yourself, if you want, as long as you put the sources under the Boost license used by POCO.

---

**Q:** How can I get write access to the Git repository?

**A:** For simplicity purposes, we keep the direct write access to the main repository within a small group of core contributors. Since git forks , merges and pull requests are very easy and simple, this was not an obstacle so far. If you believe you really, really need write access to main repo, please contact the project maintainers at poco@pocoproject.org.

---

**Q:** Why does compilation or test run fail on MinGW, Cygwin, [your platform of choice here]?

**A:** Because we have limited manpower/resources available and can't keep up with every minute detail of every platform. Core team makes sure reasonably recent versions of **Visual Studio**, **clang** and **gcc** build/tests pass on Windows, OSX and Linux; the rest is left to the contributors and other parties with interest in particular platforms. As its name says, POCO is very portable and typically it is a minor code fix or ifdef that is needed to iron a wrinkle out; please consider changing it yourself and send us pull request - that will make everyone happy. If you are inclined to do so, please consider "owning" a platform or build system. It is not much of a commitment - you will be expected to (**a**) keep things tidy (i.e building cleanly with tests passing) in regards to your area and (**b**) respond to user's inquiries thereof.

---
