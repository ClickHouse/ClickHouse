/**
 * Main page object containing all methods, selectors and functionality
 * that is shared across all page objects.
 */
export default class Page {

    /**
     * Opens a sub-page of the page.
     *
     * Tests check pages opened with 'file://' schema because it does
     * not require staring an HTTP server and covers most of the features.
     */
    async open(path: string) {
        await browser.url(`file://${process.env.PWD}/../${path}`);
    }
}
