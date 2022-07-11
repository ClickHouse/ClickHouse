---
title: Which ClickHouse version to use in production?
toc_hidden: true
toc_priority: 10
---

# Which ClickHouse Version to Use in Production? {#which-clickhouse-version-to-use-in-production}

First of all, let’s discuss why people ask this question in the first place. There are two key reasons:

1.  ClickHouse is developed with pretty high velocity and usually, there are 10+ stable releases per year. It makes a wide range of releases to choose from, which is not so trivial choice.
2.  Some users want to avoid spending time figuring out which version works best for their use case and just follow someone else’s advice.

The second reason is more fundamental, so we’ll start with it and then get back to navigating through various ClickHouse releases.

## Which ClickHouse Version Do You Recommend? {#which-clickhouse-version-do-you-recommend}

It’s tempting to hire consultants or trust some known experts to get rid of responsibility for your production environment. You install some specific ClickHouse version that someone else recommended, now if there’s some issue with it - it’s not your fault, it’s someone else’s. This line of reasoning is a big trap. No external person knows better what’s going on in your company’s production environment.

So how to properly choose which ClickHouse version to upgrade to? Or how to choose your first ClickHouse version? First of all, you need to invest in setting up a **realistic pre-production environment**. In an ideal world, it could be a completely identical shadow copy, but that’s usually expensive.

Here’re some key points to get reasonable fidelity in a pre-production environment with not so high costs:

-   Pre-production environment needs to run an as close set of queries as you intend to run in production:
    -   Don’t make it read-only with some frozen data.
    -   Don’t make it write-only with just copying data without building some typical reports.
    -   Don’t wipe it clean instead of applying schema migrations.
-   Use a sample of real production data and queries. Try to choose a sample that’s still representative and makes `SELECT` queries return reasonable results. Use obfuscation if your data is sensitive and internal policies do not allow it to leave the production environment.
-   Make sure that pre-production is covered by your monitoring and alerting software the same way as your production environment does.
-   If your production spans across multiple datacenters or regions, make your pre-production does the same.
-   If your production uses complex features like replication, distributed table, cascading materialize views, make sure they are configured similarly in pre-production.
-   There’s a trade-off on using the roughly same number of servers or VMs in pre-production as in production, but of smaller size, or much less of them, but of the same size. The first option might catch extra network-related issues, while the latter is easier to manage.

The second area to invest in is **automated testing infrastructure**. Don’t assume that if some kind of query has executed successfully once, it’ll continue to do so forever. It’s ok to have some unit tests where ClickHouse is mocked but make sure your product has a reasonable set of automated tests that are run against real ClickHouse and check that all important use cases are still working as expected.

Extra step forward could be contributing those automated tests to [ClickHouse’s open-source test infrastructure](https://github.com/ClickHouse/ClickHouse/tree/master/tests) that’s continuously used in its day-to-day development. It definitely will take some additional time and effort to learn [how to run it](../../development/tests.md) and then how to adapt your tests to this framework, but it’ll pay off by ensuring that ClickHouse releases are already tested against them when they are announced stable, instead of repeatedly losing time on reporting the issue after the fact and then waiting for a bugfix to be implemented, backported and released. Some companies even have such test contributions to infrastructure by its use as an internal policy, most notably it’s called [Beyonce’s Rule](https://www.oreilly.com/library/view/software-engineering-at/9781492082781/ch01.html#policies_that_scale_well) at Google.

When you have your pre-production environment and testing infrastructure in place, choosing the best version is straightforward:

1.  Routinely run your automated tests against new ClickHouse releases. You can do it even for ClickHouse releases that are marked as `testing`, but going forward to the next steps with them is not recommended.
2.  Deploy the ClickHouse release that passed the tests to pre-production and check that all processes are running as expected.
3.  Report any issues you discovered to [ClickHouse GitHub Issues](https://github.com/ClickHouse/ClickHouse/issues).
4.  If there were no major issues, it should be safe to start deploying ClickHouse release to your production environment. Investing in gradual release automation that implements an approach similar to [canary releases](https://martinfowler.com/bliki/CanaryRelease.html) or [green-blue deployments](https://martinfowler.com/bliki/BlueGreenDeployment.html) might further reduce the risk of issues in production.

As you might have noticed, there’s nothing specific to ClickHouse in the approach described above, people do that for any piece of infrastructure they rely on if they take their production environment seriously.

## How to Choose Between ClickHouse Releases? {#how-to-choose-between-clickhouse-releases}

If you look into contents of ClickHouse package repository, you’ll see four kinds of packages:

1.  `testing`
2.  `prestable`
3.  `stable`
4.  `lts` (long-term support)

As was mentioned earlier, `testing` is good mostly to notice issues early, running them in production is not recommended because each of them is not tested as thoroughly as other kinds of packages.

`prestable` is a release candidate which generally looks promising and is likely to become announced as `stable` soon. You can try them out in pre-production and report issues if you see any.

For production use, there are two key options: `stable` and `lts`. Here is some guidance on how to choose between them:

-   `stable` is the kind of package we recommend by default. They are released roughly monthly (and thus provide new features with reasonable delay) and three latest stable releases are supported in terms of diagnostics and backporting of bugfixes.
-   `lts` are released twice a year and are supported for a year after their initial release. You might prefer them over `stable` in the following cases:
    -   Your company has some internal policies that do not allow for frequent upgrades or using non-LTS software.
    -   You are using ClickHouse in some secondary products that either does not require any complex ClickHouse features and do not have enough resources to keep it updated.

Many teams who initially thought that `lts` is the way to go, often switch to `stable` anyway because of some recent feature that’s important for their product.

!!! warning "Important"
    One more thing to keep in mind when upgrading ClickHouse: we’re always keeping eye on compatibility across releases, but sometimes it’s not reasonable to keep and some minor details might change. So make sure you check the [changelog](../../whats-new/changelog/index.md) before upgrading to see if there are any notes about backward-incompatible changes.
