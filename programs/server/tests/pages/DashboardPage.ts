import Page from './Page';

class DashboardPage extends Page {
    async open() {
        await super.open('dashboard.html');
    }

    get urlInput() {return $('#url');}

    get userInput() {return $('#user');}

    get passwordInput() {return $('#password');}

    get addChartButton() {return $('#add');}

    get toggleLightThemeLink() {return $('#toggle-light');}

    get toggleDarkThemeLink() {return $('#toggle-dark');}

    get roundingInput() {return $('input[name=rounding]');}

    get secondsInput() {return $('input[name=seconds]');}

    get confirmAddChartButtons() {return $$('.query-editor input.edit-confirm');}

    get chartTitleInputs() {return $$('.query-editor input.edit-title');}

    get chartQueryInputs() {return $$('.query-editor textarea[placeholder=Query]');}

    get emptyChartCount(): Promise<number> { return $$('div.chart:not(div.uplot)').length; }

    get uplotChartCount(): Promise<number> {return $$('div.chart div.uplot canvas').length;}

    async getUplotChartByIndex(index: number): Promise<WebdriverIO.Element | undefined> {
        const elements = await $$('div.uplot canvas');
        return elements.at(index);
    }

}

export default new DashboardPage();
