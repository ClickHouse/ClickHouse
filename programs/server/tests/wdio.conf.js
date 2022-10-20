// noinspection JSUnusedGlobalSymbols

/**
 * See: https://webdriver.io/docs/configurationfile/
 *
 * Using '.js' file format instead of .ts because in Intellij IDEA when running/debugging an individual test
 * the 'wdio' plugin looks for '.js' config file by default.
 */
exports.config = {
    autoCompileOpts: {
        autoCompile: true,
        tsNodeOpts: {
            transpileOnly: true,
            project: 'tsconfig.json'
        }
    },
    specs: [
        './specs/**/*.ts'
    ],
    maxInstances: 1,
    capabilities: [{
        browserName: 'chrome',
        acceptInsecureCerts: true,
        'goog:chromeOptions': {'args': ['disable-extensions', 'headless', 'disable-gpu', 'no-sandbox']}
    }],
    logLevel: 'info',
    bail: 0,
    baseUrl: 'http://localhost',
    waitforTimeout: 10000,
    connectionRetryTimeout: 120000,
    connectionRetryCount: 3,
    services: ['chromedriver'],
    framework: 'mocha',
    reporters: ['spec'],
    mochaOpts: {
        ui: 'bdd',
        timeout: 60000
    },
}
