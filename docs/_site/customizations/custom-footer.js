(function () {
  'use strict';

  var FOOTER_ID = 'ch-custom-footer';

  var columns = [
    {
      title: 'Product',
      links: [
        ['ClickHouse Cloud', 'https://clickhouse.com/cloud'],
        ['Bring Your Own Cloud', 'https://clickhouse.com/cloud/bring-your-own-cloud'],
        ['Postgres managed by ClickHouse', 'https://clickhouse.com/cloud/postgres'],
        ['Managed ClickStack', 'https://clickhouse.com/cloud/clickstack'],
        ['ClickHouse', 'https://clickhouse.com/clickhouse'],
        ['ClickStack', 'https://clickhouse.com/clickstack'],
        ['Agentic Data Stack', 'https://clickhouse.com/ai'],
        ['ClickHouse Government', 'https://clickhouse.com/government'],
        ['ClickHouse Keeper', 'https://clickhouse.com/clickhouse/keeper'],
        ['ClickPipes', 'https://clickhouse.com/cloud/clickpipes'],
        ['Integrations', 'https://clickhouse.com/integrations'],
        ['chDB', 'https://clickhouse.com/chdb'],
        ['Pricing', 'https://clickhouse.com/pricing']
      ]
    },
    {
      title: 'Resources',
      links: [
        ['Documentation', 'https://clickhouse.com/docs'],
        ['Trust Center', 'https://trust.clickhouse.com'],
        ['Training', 'https://clickhouse.com/learn'],
        ['Support', 'https://clickhouse.com/support/program'],
        ['Benchmarks', 'https://clickhouse.com/benchmarks'],
        ['Use Cases', 'https://clickhouse.com/use-cases'],
        ['Videos', 'https://clickhouse.com/videos'],
        ['Demos', 'https://clickhouse.com/demos'],
        ['Presentations', 'https://presentations.clickhouse.com/'],
        ['Real-time Data Warehouse', 'https://clickhouse.com/real-time-data-warehouse'],
        ['Engineering Resources', 'https://clickhouse.com/resources/engineering']
      ]
    },
    {
      title: 'Company',
      links: [
        ['Blog', 'https://clickhouse.com/blog'],
        ['Our Story', 'https://clickhouse.com/company/our-story'],
        ['Careers', 'https://clickhouse.com/company/careers'],
        ['Contact Us', 'https://clickhouse.com/company/contact'],
        ['Events', 'https://clickhouse.com/company/events'],
        ['News', 'https://clickhouse.com/company/news'],
        ['Media', 'https://clickhouse.com/media']
      ]
    },
    {
      title: 'Join our community',
      links: [
        ['GitHub', 'https://github.com/ClickHouse/ClickHouse'],
        ['Slack', 'https://clickhouse.com/slack'],
        ['LinkedIn', 'https://www.linkedin.com/company/clickhouseinc'],
        ['X', 'https://x.com/ClickHouseDB'],
        ['Bluesky', 'https://bsky.app/profile/clickhouse.com'],
        ['Telegram', 'https://telegram.me/clickhouse_en'],
        ['Meetup', 'https://www.meetup.com/pro/clickhouse']
      ]
    },
    {
      title: 'Comparisons',
      links: [
        ['BigQuery', 'https://clickhouse.com/comparison/bigquery'],
        ['PostgreSQL', 'https://clickhouse.com/comparison/postgresql'],
        ['Redshift', 'https://clickhouse.com/comparison/redshift'],
        ['Snowflake', 'https://clickhouse.com/comparison/snowflake'],
        ['Elastic', 'https://clickhouse.com/comparison/elastic-for-observability'],
        ['Splunk', 'https://clickhouse.com/comparison/splunk-for-observability'],
        ['OpenSearch', 'https://clickhouse.com/comparison/opensearch-for-observability']
      ],
      subsection: {
        title: 'Partners',
        links: [
          ['AWS', 'https://clickhouse.com/partners/aws'],
          ['Azure', 'https://clickhouse.com/partners/azure']
        ]
      }
    }
  ];

  var legalLinks = [
    ['Trademark', 'https://clickhouse.com/legal/trademark-policy'],
    ['Privacy', 'https://clickhouse.com/legal/privacy-policy'],
    ['Security', 'https://trust.clickhouse.com/'],
    ['Legal', 'https://clickhouse.com/legal'],
    ['Cookie Policy', 'https://clickhouse.com/legal/cookie-policy']
  ];

  var logoSvg = '<svg width="135" height="40" viewBox="0 0 135 40" fill="none" xmlns="http://www.w3.org/2000/svg">'
    + '<g clip-path="url(#clip0_378_10860)">'
    + '<rect x="2.70837" y="9.875" width="2.24992" height="20.2493" rx="0.236664" fill="white"/>'
    + '<rect x="7.2085" y="9.875" width="2.24992" height="20.2493" rx="0.236664" fill="white"/>'
    + '<rect x="11.7086" y="9.875" width="2.24992" height="20.2493" rx="0.236664" fill="white"/>'
    + '<rect x="16.2076" y="9.875" width="2.24992" height="20.2493" rx="0.236664" fill="white"/>'
    + '<rect x="20.7087" y="17.7502" width="2.24992" height="4.49985" rx="0.236664" fill="white"/>'
    + '</g>'
    + '<path d="M40.0334 15.142C39.3974 15.142 38.8274 15.256 38.3234 15.484C37.8194 15.7 37.3874 16.024 37.0274 16.456C36.6794 16.888 36.4094 17.41 36.2174 18.022C36.0374 18.634 35.9474 19.324 35.9474 20.092C35.9474 21.1 36.0974 21.976 36.3974 22.72C36.6974 23.452 37.1474 24.016 37.7474 24.412C38.3474 24.808 39.1034 25.006 40.0154 25.006C40.5674 25.006 41.0954 24.958 41.5994 24.862C42.1154 24.754 42.6374 24.616 43.1654 24.448V26.122C42.6614 26.314 42.1454 26.452 41.6174 26.536C41.0894 26.632 40.4774 26.68 39.7814 26.68C38.4734 26.68 37.3814 26.41 36.5054 25.87C35.6414 25.33 34.9934 24.562 34.5614 23.566C34.1294 22.57 33.9134 21.406 33.9134 20.074C33.9134 19.102 34.0454 18.214 34.3094 17.41C34.5854 16.594 34.9814 15.892 35.4974 15.304C36.0134 14.716 36.6494 14.266 37.4054 13.954C38.1734 13.63 39.0554 13.468 40.0514 13.468C40.6994 13.468 41.3354 13.54 41.9594 13.684C42.5834 13.816 43.1474 14.008 43.6514 14.26L42.9314 15.88C42.5114 15.688 42.0554 15.52 41.5634 15.376C41.0834 15.22 40.5734 15.142 40.0334 15.142ZM47.626 26.5H45.718V12.82H47.626V26.5ZM52.5303 16.798V26.5H50.6223V16.798H52.5303ZM51.5943 13.108C51.8823 13.108 52.1343 13.192 52.3503 13.36C52.5663 13.528 52.6743 13.81 52.6743 14.206C52.6743 14.59 52.5663 14.872 52.3503 15.052C52.1343 15.22 51.8823 15.304 51.5943 15.304C51.2823 15.304 51.0183 15.22 50.8023 15.052C50.5983 14.872 50.4963 14.59 50.4963 14.206C50.4963 13.81 50.5983 13.528 50.8023 13.36C51.0183 13.192 51.2823 13.108 51.5943 13.108ZM59.4326 26.68C58.5446 26.68 57.7646 26.506 57.0926 26.158C56.4206 25.81 55.8986 25.27 55.5266 24.538C55.1546 23.794 54.9686 22.852 54.9686 21.712C54.9686 20.512 55.1666 19.54 55.5626 18.796C55.9706 18.052 56.5166 17.506 57.2006 17.158C57.8966 16.798 58.6886 16.618 59.5766 16.618C60.1166 16.618 60.6206 16.678 61.0886 16.798C61.5686 16.906 61.9646 17.032 62.2766 17.176L61.7006 18.724C61.3646 18.592 61.0046 18.478 60.6206 18.382C60.2486 18.286 59.8946 18.238 59.5586 18.238C58.9586 18.238 58.4606 18.37 58.0646 18.634C57.6806 18.886 57.3926 19.27 57.2006 19.786C57.0206 20.29 56.9306 20.926 56.9306 21.694C56.9306 22.426 57.0266 23.044 57.2186 23.548C57.4106 24.052 57.6926 24.436 58.0646 24.7C58.4486 24.952 58.9226 25.078 59.4866 25.078C60.0266 25.078 60.5006 25.018 60.9086 24.898C61.3166 24.778 61.7006 24.622 62.0606 24.43V26.086C61.7126 26.29 61.3346 26.44 60.9266 26.536C60.5186 26.632 60.0206 26.68 59.4326 26.68ZM66.2408 19.66C66.2408 19.912 66.2288 20.2 66.2048 20.524C66.1928 20.848 66.1748 21.148 66.1508 21.424H66.2048C66.3008 21.304 66.4148 21.16 66.5468 20.992C66.6908 20.812 66.8348 20.632 66.9788 20.452C67.1228 20.272 67.2548 20.116 67.3748 19.984L70.3448 16.798H72.5588L68.6528 20.956L72.8108 26.5H70.5608L67.3568 22.162L66.2408 23.098V26.5H64.3508V12.82H66.2408V19.66ZM84.6475 26.5H82.7035V20.632H76.5655V26.5H74.6215V13.648H76.5655V18.976H82.7035V13.648H84.6475V26.5ZM96.4203 21.64C96.4203 22.444 96.3123 23.158 96.0963 23.782C95.8803 24.406 95.5743 24.934 95.1783 25.366C94.7823 25.798 94.3023 26.128 93.7383 26.356C93.1743 26.572 92.5383 26.68 91.8303 26.68C91.1823 26.68 90.5823 26.572 90.0303 26.356C89.4783 26.128 88.9983 25.798 88.5903 25.366C88.1943 24.934 87.8823 24.406 87.6543 23.782C87.4263 23.158 87.3123 22.438 87.3123 21.622C87.3123 20.554 87.4923 19.648 87.8523 18.904C88.2243 18.16 88.7523 17.596 89.4363 17.212C90.1323 16.816 90.9483 16.618 91.8843 16.618C92.7723 16.618 93.5523 16.816 94.2243 17.212C94.9083 17.596 95.4423 18.16 95.8263 18.904C96.2223 19.648 96.4203 20.56 96.4203 21.64ZM89.2743 21.64C89.2743 22.36 89.3643 22.984 89.5443 23.512C89.7243 24.028 90.0063 24.424 90.3903 24.7C90.7743 24.976 91.2663 25.114 91.8663 25.114C92.4663 25.114 92.9583 24.976 93.3423 24.7C93.7263 24.424 94.0083 24.028 94.1883 23.512C94.3683 22.984 94.4583 22.36 94.4583 21.64C94.4583 20.896 94.3623 20.272 94.1703 19.768C93.9903 19.264 93.7083 18.88 93.3243 18.616C92.9523 18.34 92.4603 18.202 91.8483 18.202C90.9483 18.202 90.2943 18.502 89.8863 19.102C89.4783 19.702 89.2743 20.548 89.2743 21.64ZM107.191 16.798V26.5H105.661L105.391 25.222H105.301C105.097 25.558 104.833 25.834 104.509 26.05C104.185 26.266 103.831 26.422 103.447 26.518C103.063 26.626 102.667 26.68 102.259 26.68C101.503 26.68 100.861 26.56 100.333 26.32C99.8171 26.068 99.4211 25.684 99.1451 25.168C98.8811 24.64 98.7491 23.968 98.7491 23.152V16.798H100.675V22.864C100.675 23.62 100.831 24.184 101.143 24.556C101.467 24.928 101.965 25.114 102.637 25.114C103.309 25.114 103.837 24.982 104.221 24.718C104.605 24.454 104.875 24.07 105.031 23.566C105.199 23.05 105.283 22.432 105.283 21.712V16.798H107.191ZM116.621 23.764C116.621 24.4 116.465 24.934 116.153 25.366C115.841 25.798 115.391 26.128 114.803 26.356C114.215 26.572 113.507 26.68 112.679 26.68C111.995 26.68 111.407 26.626 110.915 26.518C110.435 26.422 109.991 26.278 109.583 26.086V24.412C110.015 24.616 110.513 24.802 111.077 24.97C111.641 25.126 112.193 25.204 112.733 25.204C113.453 25.204 113.969 25.09 114.281 24.862C114.605 24.634 114.767 24.328 114.767 23.944C114.767 23.728 114.707 23.536 114.587 23.368C114.467 23.188 114.239 23.008 113.903 22.828C113.579 22.636 113.099 22.42 112.463 22.18C111.851 21.928 111.329 21.682 110.897 21.442C110.465 21.19 110.135 20.896 109.907 20.56C109.679 20.212 109.565 19.768 109.565 19.228C109.565 18.388 109.901 17.746 110.573 17.302C111.257 16.846 112.157 16.618 113.273 16.618C113.861 16.618 114.413 16.678 114.929 16.798C115.457 16.906 115.967 17.074 116.459 17.302L115.829 18.76C115.553 18.628 115.265 18.52 114.965 18.436C114.677 18.34 114.383 18.262 114.083 18.202C113.795 18.142 113.495 18.112 113.183 18.112C112.607 18.112 112.169 18.202 111.869 18.382C111.569 18.562 111.419 18.814 111.419 19.138C111.419 19.366 111.485 19.564 111.617 19.732C111.761 19.9 112.007 20.068 112.355 20.236C112.703 20.404 113.177 20.608 113.777 20.848C114.377 21.076 114.887 21.31 115.307 21.55C115.739 21.79 116.063 22.084 116.279 22.432C116.507 22.78 116.621 23.224 116.621 23.764ZM122.712 16.618C123.564 16.618 124.296 16.798 124.908 17.158C125.52 17.506 125.988 18.01 126.312 18.67C126.648 19.33 126.816 20.11 126.816 21.01V22.054H120.3C120.324 23.05 120.582 23.812 121.074 24.34C121.578 24.868 122.28 25.132 123.18 25.132C123.804 25.132 124.356 25.078 124.836 24.97C125.328 24.85 125.832 24.676 126.348 24.448V26.032C125.868 26.248 125.376 26.41 124.872 26.518C124.38 26.626 123.792 26.68 123.108 26.68C122.172 26.68 121.344 26.494 120.624 26.122C119.904 25.75 119.34 25.198 118.932 24.466C118.536 23.722 118.338 22.81 118.338 21.73C118.338 20.638 118.518 19.714 118.878 18.958C119.25 18.202 119.76 17.626 120.408 17.23C121.068 16.822 121.836 16.618 122.712 16.618ZM122.712 18.094C122.028 18.094 121.476 18.316 121.056 18.76C120.648 19.204 120.408 19.834 120.336 20.65H124.908C124.908 20.146 124.83 19.702 124.674 19.318C124.518 18.934 124.278 18.634 123.954 18.418C123.63 18.202 123.216 18.094 122.712 18.094Z" fill="white"/>'
    + '<defs><clipPath id="clip0_378_10860"><rect width="24" height="24" fill="white" transform="translate(0.833374 8)"/></clipPath></defs>'
    + '</svg>';

  var githubSvg = '<svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">'
    + '<path fill-rule="evenodd" clip-rule="evenodd" d="M8 1.75C4.27 1.75 1.25 4.77 1.25 8.5c0 2.99 1.93 5.51 4.62 6.4.34.06.46-.14.46-.32 0-.16-.01-.69-.01-1.26-1.7.32-2.14-.6-2.27-.98-.08-.19-.41-.79-.7-.95-.24-.13-.58-.44-.01-.45.53-.01.91.49 1.04.69.61 1.02 1.58.73 1.97.56.06-.44.24-.74.43-.91-1.5-.17-3.07-.75-3.07-3.33 0-.73.26-1.34.69-1.81-.07-.17-.3-.86.07-1.79 0 0 .57-.18 1.86.69.54-.15 1.11-.23 1.69-.23.57 0 1.14.08 1.68.23 1.29-.88 1.86-.69 1.86-.69.37.93.14 1.62.07 1.79.43.47.69 1.07.69 1.81 0 2.59-1.58 3.16-3.08 3.33.25.21.46.62.46 1.25 0 .9-.01 1.63-.01 1.86 0 .18.13.39.47.32 2.67-.9 4.58-3.41 4.58-6.4 0-3.73-3.02-6.75-6.75-6.75Z" fill="currentColor"/>'
    + '</svg>';

  function buildColumnHtml(col) {
    var html = '<div><h3>' + col.title + '</h3><ul>';
    col.links.forEach(function (link) {
      html += '<li><a href="' + link[1] + '" target="_blank" rel="noopener noreferrer">'
        + link[0] + '</a></li>';
    });
    html += '</ul>';
    if (col.subsection) {
      html += '<h3 class="ch-sub-heading">' + col.subsection.title + '</h3><ul>';
      col.subsection.links.forEach(function (link) {
        html += '<li><a href="' + link[1] + '" target="_blank" rel="noopener noreferrer">'
          + link[0] + '</a></li>';
      });
      html += '</ul>';
    }
    html += '</div>';
    return html;
  }

  function injectStyles() {
    if (document.getElementById('ch-footer-styles')) return;
    var style = document.createElement('style');
    style.id = 'ch-footer-styles';
    style.textContent = ''
      + '#' + FOOTER_ID + ' { box-sizing: border-box; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }'
      // On desktop the sidebar is fixed at 19rem wide; match #content-container's
      // horizontal padding (pl-[32px] / pr-[32px]) so the footer's inner edges
      // align with the content area used by both doc pages and the home page.
      + '@media (min-width: 1024px) { #' + FOOTER_ID + ' { padding-left: calc(19rem + 32px) !important; padding-right: 32px !important; } }'
      + '#' + FOOTER_ID + ' [data-inner] { max-width: 1280px; margin: 0 auto; }'
      + '#' + FOOTER_ID + ' * { box-sizing: border-box; }'
      + '#' + FOOTER_ID + ' a { text-decoration: none; transition: color 0.15s, border-color 0.15s; }'
      // Top section: sitemap + CTA side by side only at wide viewports
      + '#' + FOOTER_ID + ' [data-top] { display: flex; flex-direction: column; gap: 32px; padding-bottom: 48px; }'
      + '@media (min-width: 1400px) { #' + FOOTER_ID + ' [data-top] { flex-direction: row; gap: 40px; } }'
      // Sitemap grid: 2 cols default, 3 at md, 5 only when side-by-side
      + '#' + FOOTER_ID + ' [data-sitemap] { display: grid; grid-template-columns: repeat(2, 1fr); gap: 24px; flex: 1; min-width: 0; }'
      + '@media (min-width: 768px) { #' + FOOTER_ID + ' [data-sitemap] { grid-template-columns: repeat(3, 1fr); } }'
      + '@media (min-width: 1400px) { #' + FOOTER_ID + ' [data-sitemap] { grid-template-columns: repeat(5, 1fr); } }'
      // Column headings
      + '#' + FOOTER_ID + ' [data-sitemap] h3 { font-size: 13px; font-weight: 600; margin: 0 0 12px; }'
      + '#' + FOOTER_ID + ' [data-sitemap] .ch-sub-heading { margin-top: 24px; }'
      + '#' + FOOTER_ID + ' [data-sitemap] ul { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 8px; }'
      + '#' + FOOTER_ID + ' [data-sitemap] a { font-size: 13px; }'
      // CTA column
      + '#' + FOOTER_ID + ' [data-cta] { display: flex; flex-direction: column; }'
      + '@media (min-width: 1400px) { #' + FOOTER_ID + ' [data-cta] { width: 300px; flex-shrink: 0; } }'
      + '#' + FOOTER_ID + ' [data-gh] { display: inline-flex; align-items: center; justify-content: center; gap: 8px; background: transparent; padding: 10px 20px; border-radius: 8px; font-size: 13px; font-weight: 600; width: 100%; }'
      + '#' + FOOTER_ID + ' [data-gh]:hover { border-color: #888 !important; }'
      // Bottom bar
      + '#' + FOOTER_ID + ' [data-divider] { margin-bottom: 24px; }'
      + '#' + FOOTER_ID + ' [data-bottom] { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; }'
      + '#' + FOOTER_ID + ' [data-bottom] a { font-size: 13px; white-space: nowrap; }'
      + '#' + FOOTER_ID + ' [data-legal] { display: flex; flex-wrap: wrap; gap: 16px; }'
      + '@media (max-width: 639px) {'
      + '  #' + FOOTER_ID + ' [data-bottom] { flex-direction: column; text-align: center; }'
      + '  #' + FOOTER_ID + ' [data-legal] { justify-content: center; }'
      + '}'
      // Light mode colors
      + '#' + FOOTER_ID + ' [data-sitemap] h3 { color: #111; }'
      + '#' + FOOTER_ID + ' [data-sitemap] a { color: #6b7280; }'
      + '#' + FOOTER_ID + ' [data-sitemap] a:hover { color: #111; }'
      + '#' + FOOTER_ID + ' [data-gh] { color: #111; border: 1px solid #d1d5db; }'
      + '#' + FOOTER_ID + ' [data-divider] { border-top: 1px solid #e5e7eb; }'
      + '#' + FOOTER_ID + ' [data-copyright] { font-size: 13px; color: #6b7280; }'
      + '#' + FOOTER_ID + ' [data-bottom] a { color: #6b7280; }'
      + '#' + FOOTER_ID + ' [data-bottom] a:hover { color: #111; }'
      + '#' + FOOTER_ID + ' [data-logo] { margin-bottom: 16px; }'
      + '#' + FOOTER_ID + ' [data-logo] svg * { fill: #111; }'
      // Dark mode colors
      + '.dark #' + FOOTER_ID + ' [data-sitemap] h3 { color: #f5f5f5; }'
      + '.dark #' + FOOTER_ID + ' [data-sitemap] a { color: #a3a3a3; }'
      + '.dark #' + FOOTER_ID + ' [data-sitemap] a:hover { color: #f5f5f5; }'
      + '.dark #' + FOOTER_ID + ' [data-gh] { color: #fff; border-color: #555; }'
      + '.dark #' + FOOTER_ID + ' [data-divider] { border-color: #333; }'
      + '.dark #' + FOOTER_ID + ' [data-copyright] { color: #a3a3a3; }'
      + '.dark #' + FOOTER_ID + ' [data-bottom] a { color: #a3a3a3; }'
      + '.dark #' + FOOTER_ID + ' [data-bottom] a:hover { color: #f5f5f5; }'
      + '.dark #' + FOOTER_ID + ' [data-logo] svg * { fill: #fff; }';
    document.head.appendChild(style);
  }

  function buildFooterHtml() {
    var year = new Date().getFullYear();

    // Build columns HTML
    var columnsHtml = '';
    columns.forEach(function (col) {
      columnsHtml += buildColumnHtml(col);
    });

    // Build legal links HTML
    var legalHtml = '';
    legalLinks.forEach(function (link) {
      legalHtml += '<a href="' + link[1] + '" target="_blank" rel="noopener noreferrer">'
        + link[0] + '</a>';
    });

    return '<div data-inner>'
      // Top: sitemap grid + CTA column side by side on desktop
      + '<div data-top>'
        + '<div data-sitemap>' + columnsHtml + '</div>'
        + '<div data-cta>'
          + '<div data-logo>' + logoSvg + '</div>'
          + '<a data-gh href="https://github.com/ClickHouse/ClickHouse" target="_blank" rel="noopener noreferrer">'
            + githubSvg + 'Star us on Github</a>'
        + '</div>'
      + '</div>'
      // Divider
      + '<div data-divider></div>'
      // Bottom
      + '<div data-bottom>'
        + '<div data-copyright>'
          + '\u00A9 ' + year + ' ClickHouse, Inc. HQ in the Bay Area, CA and Amsterdam, NL.'
        + '</div>'
        + '<div data-legal>' + legalHtml + '</div>'
      + '</div>'
    + '</div>';
  }

  function findFooterTarget() {
    var contentContainer = document.getElementById('content-container');
    if (!contentContainer) return null;
    // Walk up to the nearest block-level ancestor so the footer renders as a
    // full-width block below the sidebar + content row. Some intermediate
    // ancestors use display:contents on mobile, so keep walking until we
    // find a real block-level container.
    var ancestor = contentContainer.parentElement;
    while (ancestor && ancestor !== document.body) {
      if (getComputedStyle(ancestor).display === 'block') {
        return ancestor;
      }
      ancestor = ancestor.parentElement;
    }
    return contentContainer;
  }

  function ensureFooterAtEnd() {
    var existing = document.getElementById(FOOTER_ID);
    if (!existing) return false;
    var target = findFooterTarget();
    // If the target was replaced or the footer drifted, move it to the end of the right parent.
    if (target && (existing.parentElement !== target || existing !== target.lastElementChild)) {
      target.appendChild(existing);
    }
    return true;
  }

  function injectFooter() {
    // Already injected — just make sure it's still positioned at the end.
    if (document.getElementById(FOOTER_ID)) {
      return ensureFooterAtEnd();
    }

    // Find the scrollable content container
    var contentContainer = document.getElementById('content-container');
    if (!contentContainer) return false;

    // Hide the existing Mintlify footer if present
    var mintlifyFooter = document.getElementById('footer');
    if (mintlifyFooter) {
      mintlifyFooter.style.display = 'none';
    }

    // Inject styles and create footer
    injectStyles();

    var wrapper = document.createElement('footer');
    wrapper.id = FOOTER_ID;
    wrapper.style.cssText = 'width:100%;padding:64px 24px 32px;';
    wrapper.innerHTML = buildFooterHtml();

    // In the Maple theme, #content-container is a flex-row, so appending
    // there makes the footer a horizontal sibling of the content columns.
    // Walk up to the nearest block-level ancestor so the footer renders as
    // a full-width block below the sidebar + content row.
    var target = findFooterTarget() || contentContainer;
    target.appendChild(wrapper);
    return true;
  }

  function init() {
    injectFooter();

    var scheduled = false;
    var observer;

    function check() {
      scheduled = false;
      var existing = document.getElementById(FOOTER_ID);
      if (!existing) {
        injectFooter();
        return;
      }
      var target = findFooterTarget();
      if (!target) return;
      if (existing.parentElement === target && existing === target.lastElementChild) return;
      // Disconnect around our own re-append so the resulting mutation doesn't
      // re-fire the observer mid-React-render, which corrupts the top-nav
      // dropdown's SVG icons during reconciliation.
      observer.disconnect();
      target.appendChild(existing);
      observer.observe(document.documentElement, { childList: true, subtree: true });
    }

    observer = new MutationObserver(function () {
      if (scheduled) return;
      scheduled = true;
      requestAnimationFrame(check);
    });
    observer.observe(document.documentElement, { childList: true, subtree: true });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
