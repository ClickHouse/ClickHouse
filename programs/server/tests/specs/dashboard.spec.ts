// noinspection JSJQueryEfficiency

import DashboardPage from '../pages/DashboardPage';

/** Default username that works for tests. */
const defaultUsername = 'play';

/** Count of charts shown by default. */
const defaultChartCount = 17;

/** Tests for the 'dashboard.html' application. */
describe('dashboard.html', () => {

    beforeEach(async () => {
        browser.executeScript('window.localStorage().clear()', []);
        await DashboardPage.open();
    });

    it('shows expected elements on the page', async () => {
        await expect(await DashboardPage.urlInput.getValue()).toBe('https://play.clickhouse.com/');
        await expect(await DashboardPage.userInput.getValue()).toBe('explorer');
        await expect(await DashboardPage.passwordInput.getValue()).toBe('');
        await expect(await DashboardPage.roundingInput.getValue()).toBe('60');
        await expect(await DashboardPage.secondsInput.getValue()).toBe('86400');
        await expect(await DashboardPage.emptyChartCount).toBe(defaultChartCount);
    });

    it('theme buttons work and keep settings on reload ', async () => {
        let html = await $('html');
        await expect(await html.getAttribute('data-theme')).toBe(null);

        await DashboardPage.toggleDarkThemeLink.click();
        await expect(await html.getAttribute('data-theme')).toBe('dark');

        await DashboardPage.toggleLightThemeLink.click();
        await expect(await html.getAttribute('data-theme')).toBe('light');

        await DashboardPage.toggleDarkThemeLink.click();
        await expect(await html.getAttribute('data-theme')).toBe('dark');

        // Reload the page.
        await DashboardPage.open();
        html = await $('html');
        await expect(await html.getAttribute('data-theme')).toBe('dark');
    });

    it('fetches chart data and shows charts', async () => {
        await expect(await DashboardPage.emptyChartCount).toBe(defaultChartCount);
        await enterUsername();
        await browser.waitUntil(async () => await DashboardPage.uplotChartCount === defaultChartCount);

        // Reload page and check that username is preserved and charts are shown.
        const url = await browser.getUrl();
        const pageSettings = parseSettingsFromUrl(url);
        expect(pageSettings).toEqual({
            'host': 'https://play.clickhouse.com/',
            'user': 'play',
            'queries': [
                {
                    'title': 'Queries/second',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_Query)\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'CPU Usage (cores)',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUVirtualTimeMicroseconds) / 1000000\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'Queries Running',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(CurrentMetric_Query)\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'Merges Running',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(CurrentMetric_Merge)\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'Selected Bytes/second',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_SelectedBytes)\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'IO Wait',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSIOWaitMicroseconds) / 1000000\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'CPU Wait',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSCPUWaitMicroseconds) / 1000000\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'OS CPU Usage (Userspace)',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)\nFROM system.asynchronous_metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = \'OSUserTimeNormalized\'\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'OS CPU Usage (Kernel)',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)\nFROM system.asynchronous_metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = \'OSSystemTimeNormalized\'\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'Read From Disk',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSReadBytes)\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'Read From Filesystem',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_OSReadChars)\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'Memory (tracked)',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(CurrentMetric_MemoryTracking)\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'Load Average (15 minutes)',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)\nFROM system.asynchronous_metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = \'LoadAverage15\'\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'Selected Rows/second',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_SelectedRows)\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'Inserted Rows/second',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(ProfileEvent_InsertedRows)\nFROM system.metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'Total MergeTree Parts',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, avg(value)\nFROM system.asynchronous_metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = \'TotalPartsOfMergeTreeTables\'\nGROUP BY t\nORDER BY t'
                }, {
                    'title': 'Max Parts For Partition',
                    'query': 'SELECT toStartOfInterval(event_time, INTERVAL {rounding:UInt32} SECOND)::INT AS t, max(value)\nFROM system.asynchronous_metric_log\nWHERE event_date >= toDate(now() - {seconds:UInt32}) AND event_time >= now() - {seconds:UInt32}\nAND metric = \'MaxPartCountForPartition\'\nGROUP BY t\nORDER BY t'
                }],
            'params': {'rounding': '60', 'seconds': '86400'}
        });
        browser.url(url);
        await browser.waitUntil(async () => await DashboardPage.uplotChartCount === defaultChartCount);
    });

    it('allows to delete charts', async () => {
        await enterUsername();
        await browser.waitUntil(async () => await DashboardPage.uplotChartCount === defaultChartCount);
        const firstChart = await DashboardPage.getUplotChartByIndex(0);

        // Hover over the plot to reveal buttons.
        firstChart.moveTo({xOffset: 20, yOffset: 20});
        const closeButtonSelector = 'a*=✕';
        await $(closeButtonSelector).click();
        await browser.waitUntil(async () => await DashboardPage.uplotChartCount === defaultChartCount - 1);
    });

    it('allows to add charts', async () => {
        await enterUsername();
        await browser.waitUntil(async () => await DashboardPage.uplotChartCount === defaultChartCount);
        await DashboardPage.addChartButton.click();
        const titleInputs = await DashboardPage.chartTitleInputs;
        await titleInputs[titleInputs.length - 1].setValue('Hello Chart');
        const queryInputs = await DashboardPage.chartQueryInputs;
        await queryInputs[queryInputs.length - 1].setValue('SELECT 4, 2');
        const confirmButtons = await DashboardPage.confirmAddChartButtons;
        await confirmButtons[confirmButtons.length - 1].click();
        await browser.waitUntil(async () => await DashboardPage.uplotChartCount === defaultChartCount + 1);
    });

    it('allows to edit charts', async () => {
        await enterUsername();
        await browser.waitUntil(async () => await DashboardPage.uplotChartCount === defaultChartCount);
        const firstChart = (await DashboardPage.getUplotChartByIndex(0)).parentElement();

        // Hover over the plot to reveal buttons.
        firstChart.moveTo({xOffset: 20, yOffset: 20});
        const editButtonSelector = 'a*=✎';
        await $(editButtonSelector).click();
        const newChartTitle = 'Hello Chart';
        const newChartQuery = 'SELECT 4, 2';
        await firstChart.$('input.edit-title').setValue(newChartTitle);
        await firstChart.$('textarea[placeholder=Query]').setValue(newChartQuery);
        await firstChart.$('input.edit-confirm').click();
        const pageSettings = parseSettingsFromUrl(await browser.getUrl());
        const pageSettingsAsString = JSON.stringify(pageSettings);
        expect(pageSettingsAsString).toContain(newChartTitle);
        expect(pageSettingsAsString).toContain(newChartQuery);
    });

});

/** Enters username into the user input and waits until all charts are loaded. */
async function enterUsername(username = defaultUsername) {
    await DashboardPage.userInput.setValue(username);
    await browser.keys(['Enter']);
}

/** Parses a serialized JSON object after # symbol in the url. Returns a JS object */
function parseSettingsFromUrl(url: string): object | undefined {
    const pageSettingsJson = Buffer.from(url.substring(url.indexOf('#') + 1), 'base64').toString();
    const result = JSON.parse(pageSettingsJson);
    return typeof result === 'object' ? result : undefined;
}

