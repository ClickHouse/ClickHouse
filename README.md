<div align=center>

[![Website](https://img.shields.io/website?up_message=AVAILABLE&down_message=DOWN&url=https://docs.altinity.com/altinitystablebuilds&style=for-the-badge)](https://docs.altinity.com/altinitystablebuilds/)
[![Apache 2.0 License](https://img.shields.io/badge/license-Apache%202.0-blueviolet?style=for-the-badge)](https://www.apache.org/licenses/LICENSE-2.0)

<picture align=center>
    <source media="(prefers-color-scheme: dark)" srcset="/docs/logo_horizontal_blue_white.png">
    <source media="(prefers-color-scheme: light)" srcset="/docs/logo_horizontal_blue_black.png">
    <img alt="Altinity company logo" src="/docs/logo_horizontal_blue_black.png">
</picture>

<h1>Altinity Stable Builds®</h1>

</div>

**Altinity Stable Builds** are releases of ClickHouse® that undergo rigorous testing to verify they are secure and ready for production use. Among other things, they are: 

* Supported for three years
* Validated against client libraries and visualization tools
* Tested for cloud use, including Kubernetes
* 100% open source and 100% compatible with ClickHouse upstream builds
* Available in FIPS-compatible versions

**We encourage you to use Altinity Stable Builds whether you're an Altinity Support customer or not.**

## Acknowledgement

We at Altinity, Inc. are thoroughly grateful to the worldwide ClickHouse community, including the core committers who make ClickHouse the world's best analytics database. 

## What should I do if I find a bug in an Altinity Stable Build?

ClickHouse’s thousands of core features are all well-tested and stable. To maintain that stability, Altinity Stable Builds are built with a CI system that runs tens of thousands of tests against every commit. But of course, things can always go wrong. If that happens, let us know! **We stand behind our work.**

### If you're an Altinity customer:

1. [Contact Altinity support](https://docs.altinity.com/support/) to file an issue.

### If you're not an Altinity customer:

1. Try to upgrade to the latest bugfix release. If you’re using v23.8.8 but you know that v23.8.11.29 exists, start by upgrading to the bugfix. Upgrades to the latest maintenance releases are smooth and safe.
2. Look for similar issues in the [Altinity/ClickHouse](https://github.com/Altinity/ClickHouse/issues) or [ClickHouse/ClickHouse](https://github.com/ClickHouse/ClickHouse/issues) repos; it's possible the problem has been logged and a fix is on the way.
3. If you can reproduce the bug, try to isolate it. For example, remove pieces of a failing query one by one, creating the simplest scenario where the error still occurs. Creating [a minimal reproducible example](https://stackoverflow.com/help/minimal-reproducible-example) is a huge step towards a solution.
4. [File an issue](https://github.com/Altinity/ClickHouse/issues/new/choose) in the Altinity/ClickHouse repo.

## Useful Links

* [Release notes](https://docs.altinity.com/releasenotes/altinity-stable-release-notes/) - Complete details on the changes and fixes in each Altinity Stable Build release
* [Builds page](https://builds.altinity.cloud/) - Download and installation instructions for Altinity Stable Builds
* [Dockerhub page](https://hub.docker.com/r/altinity/clickhouse-server) - Home of the Altinity Stable Build container images
* [Knowledge base](https://kb.altinity.com/) - Insight, knowledge, and advice from the Altinity Engineering and Support teams
* [Documentation](https://docs.altinity.com/altinitystablebuilds/) - A list of current releases and their lifecycles
* [Altinity support for ClickHouse](https://altinity.com/clickhouse-support/) - The best ClickHouse support in the industry, delivered by the most knowledgeable ClickHouse team in the industry
* [Altinity administrator training for ClickHouse](https://altinity.com/clickhouse-training/) - Understand how ClickHouse works, not just how to use it
* [Altinity blog](https://altinity.com/blog/) - The latest news on Altinity's ClickHouse-related projects, release notes, tutorials, and more
* [Altinity YouTube channel](https://www.youtube.com/@AltinityB) - ClickHouse tutorials, webinars, conference presentations, and other useful things
* [Altinity Slack channel](https://altinitydbworkspace.slack.com/join/shared_invite/zt-1togw9b4g-N0ZOXQyEyPCBh_7IEHUjdw#/shared-invite/email) - The Altinity Slack channel

<hr>

*©2024 Altinity Inc. All rights reserved. Altinity®, Altinity.Cloud®, and Altinity Stable Builds® are registered trademarks of Altinity, Inc. ClickHouse® is a registered trademark of ClickHouse, Inc.*
