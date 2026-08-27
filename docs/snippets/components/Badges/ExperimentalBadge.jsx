
const Icon = () => {
    return (
        <div className="experimentalIcon">
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
                <path strokeWidth="1.25" d="M5.5 2H10.5" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"/>
                <path strokeWidth="1.25" d="M9.50015 2V6.19625L13.4283 12.7425C13.4738 12.8183 13.4985 12.9049 13.4996 12.9934C13.5008 13.0818 13.4785 13.169 13.435 13.246C13.3914 13.323 13.3283 13.3871 13.2519 13.4317C13.1755 13.4764 13.0886 13.4999 13.0002 13.5H3.00015C2.91164 13.5 2.8247 13.4766 2.74822 13.432C2.67174 13.3874 2.60847 13.3233 2.56487 13.2463C2.52126 13.1693 2.49889 13.082 2.50004 12.9935C2.50119 12.905 2.52582 12.8184 2.5714 12.7425L6.50015 6.19625V2" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"/>
                <path strokeWidth="1.25" d="M4.47656 9.56754C5.30344 9.41254 6.47656 9.47942 7.99969 10.25C10.0153 11.2707 11.4216 11.0569 12.2184 10.7282" stroke="currentColor" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
        </div>
    )
}

export const ExperimentalBadge = () => {
    return (
        <div className="experimentalBadge">
            <Icon />Experimental feature.&nbsp;<u><a href='/docs/beta-and-experimental-features#experimental-features'>Learn more.</a></u>
        </div>
    )
}
